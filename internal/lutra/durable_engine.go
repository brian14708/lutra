package lutra

import (
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"time"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/result"
	"github.com/brian14708/lutra/internal/runlog"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/server"
)

// V0 is unstable: journal interpretation may change during development.
const (
	actionService = "LutraActionV0"
	cacheService  = "LutraCacheV0"
)

type DurableAdapter struct {
	DB       *pgxpool.Pool
	Logs     runlog.Service
	Tasks    *SandboxRuntime
	Dispatch *Dispatcher
}

func (w *DurableAdapter) Handler() (http.Handler, error) {
	return server.NewRestate().Bidirectional(true).
		Bind(restate.NewWorkflow(actionService, restate.WithInactivityTimeout(30*time.Second)).
			Handler("Run", restate.NewWorkflowHandler(w.runAction, restate.WithBinary)).
			Handler("Signal", restate.NewWorkflowSharedHandler(w.signal)).
			Handler("Inspect", restate.NewWorkflowSharedHandler(w.inspectWorkflow))).
		Bind(restate.NewObject(awakeableService).Handler("Resolve", restate.NewObjectHandler(w.resolveAwakeable))).
		Bind(restate.NewObject(cacheService).Handler("Run", restate.NewObjectHandler(w.cached))).Handler()
}

type actionData struct {
	Row    db.LoadActionRow
	Config RunConfigSnapshot
}

func (w *DurableAdapter) loadAction(ctx context.Context, id uuid.UUID) (actionData, error) {
	q := db.New(w.DB)
	row, err := q.LoadAction(ctx, id)
	if err != nil {
		return actionData{}, err
	}
	config, err := loadRunConfig(ctx, q, row.RunID)
	if err != nil {
		return actionData{}, err
	}
	return actionData{row, config}, nil
}

func (w *DurableAdapter) runAction(ctx restate.WorkflowContext, _ restate.Void) ([]byte, error) {
	id, err := uuid.Parse(restate.Key(ctx))
	if err != nil {
		return nil, restate.TerminalErrorf("invalid action ID")
	}
	data, err := restate.Run(ctx, func(ctx restate.RunContext) (actionData, error) { return w.loadAction(ctx, id) }, restate.WithName("Load action and configuration"))
	if err != nil {
		return nil, err
	}
	if terminal(string(data.Row.Status)) {
		return data.Row.ResultCbor, nil
	}
	task, action, err := environmentExecution(data.Row, data.Config)
	if err != nil {
		return nil, restate.ToTerminalError(err)
	}
	var outcome executionResult
	if task.Spec.Entrypoints[task.EntrypointID-1].GetWorkflow() {
		image, buildErr := w.prepareAction(ctx, task, data.Row.ImageKey)
		if buildErr != nil {
			outcome.Output = failureOutput(buildErr)
		} else {
			runner := WorkflowRunner{Adapter: w, Tasks: w.Tasks}
			output, runErr := runner.workflow(ctx, image, task)
			outcome.Output = output
			if runErr != nil {
				if !restate.IsTerminalError(runErr) {
					return nil, runErr
				}
				outcome.Output = failureOutput(runErr)
			}
		}
	} else if action.Cache {
		runtime, runtimeErr := w.Tasks.runtime()
		if runtimeErr != nil {
			return nil, runtimeErr
		}
		key := cacheKey(data.Row, action, runtime, data.Config)
		if len(key) != 32 {
			return nil, restate.TerminalErrorf("invalid cache key")
		}
		outcome, err = restate.Object[executionResult](ctx, cacheService, hex.EncodeToString(key), "Run").Request(id.String())
		if err != nil {
			outcome.Output = failureOutput(err)
		}
	} else {
		outcome.Output = w.effect(ctx, data, task, action)
	}
	if err := restate.RunVoid(ctx, func(ctx restate.RunContext) error {
		return w.finish(ctx, id, outcome)
	}, restate.WithName("Record action result")); err != nil {
		return nil, err
	}
	return outcome.Output, nil
}

type executionResult struct {
	Output   []byte
	CacheHit bool
}

func (w *DurableAdapter) cached(ctx restate.ObjectContext, actionID string) (executionResult, error) {
	var err error // restate results are TerminalError; keep err a plain error
	output, err := restate.Get[[]byte](ctx, "output", restate.WithBinary)
	if err != nil {
		return executionResult{}, err
	}
	if output != nil {
		return executionResult{output, true}, nil
	}
	id, err := uuid.Parse(actionID)
	if err != nil {
		return executionResult{}, restate.ToTerminalError(err)
	}
	data, err := restate.Run(ctx, func(ctx restate.RunContext) (actionData, error) { return w.loadAction(ctx, id) }, restate.WithName("Load cache owner action"))
	if err != nil {
		return executionResult{}, err
	}
	task, spec, err := environmentExecution(data.Row, data.Config)
	if err != nil {
		return executionResult{}, restate.ToTerminalError(err)
	}
	output = w.effect(ctx, data, task, spec)
	failure, tagged, err := result.DecodeFailure(output)
	if err != nil {
		return executionResult{}, restate.ToTerminalError(err)
	}
	if !tagged || failure.Cacheable {
		restate.Set(ctx, "output", output, restate.WithBinary)
	}
	return executionResult{Output: output}, nil
}

func (w *DurableAdapter) effect(ctx restate.Context, data actionData, task *EnvironmentExecution, spec *lutrav1.ActionSpec) []byte {
	image, err := w.prepareAction(ctx, task, data.Row.ImageKey)
	if err != nil {
		return failureOutput(err)
	}
	attempts := max(spec.MaxAttempts, 1)
	for attempt := int32(1); ; attempt++ {
		outcome, err := restate.Run(ctx, func(ctx restate.RunContext) (taskOutcome, error) {
			executionCtx, cancel := w.actionContext(ctx, data.Row.ID)
			defer cancel()
			release, err := w.Tasks.acquire(executionCtx, LogTask)
			if err != nil {
				return taskOutcome{}, err
			}
			defer release()
			task.Attempt, err = w.begin(executionCtx, data.Row.ID)
			if err != nil {
				return taskOutcome{}, err
			}
			ctx.Log().Info("execute task", "action_id", data.Row.ID, "attempt", task.Attempt)
			identity := TaskIdentity{RunID: data.Row.RunID, ActionID: data.Row.ID, Attempt: task.Attempt}
			output, err := w.Tasks.executeEnvironment(executionCtx, image, identity, task, w.Tasks.newTaskAPIHandler(identity))
			var taskErr *TaskError
			var terminalErr *TerminalTaskError
			var cacheable *CacheableError
			var configErr *ConfigError
			switch {
			case errors.As(err, &terminalErr):
				return taskOutcome{Output: output}, nil
			case errors.As(err, &taskErr):
				return taskOutcome{Output: failureOutput(err), Retry: true}, nil
			case errors.As(err, &cacheable):
				output, err = cacheable.output()
			case errors.As(err, &configErr):
				return taskOutcome{}, restate.ToTerminalError(err)
			}
			if executionCtx.Err() != nil && ctx.Err() == nil {
				return taskOutcome{}, restate.TerminalErrorf("action canceled")
			}
			return taskOutcome{Output: output}, err
		}, restate.WithName("Execute task sandbox"), restate.WithMaxRetryAttempts(infrastructureAttempts))
		if err != nil {
			return failureOutput(err)
		}
		if !outcome.Retry || attempt == attempts {
			return outcome.Output
		}
	}
}

type taskOutcome struct {
	Output []byte
	Retry  bool
}

// Runtime and build errors have their own bounded budget. A build command's
// exit status cannot distinguish a broken recipe from a temporary download error.
const infrastructureAttempts = 5

func (w *DurableAdapter) prepareAction(ctx restate.Context, task *EnvironmentExecution, key []byte) (*Image, error) {
	return restate.Run(ctx, func(ctx restate.RunContext) (*Image, error) {
		buildCtx, cancel := w.actionContext(ctx, uuid.MustParse(task.ActionID))
		defer cancel()
		release, err := w.Tasks.acquire(buildCtx, LogBuild)
		if err != nil {
			return nil, err
		}
		defer release()
		if err := w.project(buildCtx, uuid.MustParse(task.ActionID), db.LutraTaskActionStatusBuilding, nil, false); err != nil {
			return nil, err
		}
		image, err := w.Tasks.prepare(buildCtx, task, key)
		if buildCtx.Err() != nil && ctx.Err() == nil {
			return nil, restate.TerminalErrorf("action canceled")
		}
		switch connect.CodeOf(err) {
		case connect.CodeInvalidArgument, connect.CodeFailedPrecondition, connect.CodeUnimplemented:
			return nil, restate.ToTerminalError(err)
		}
		return image, err
	}, restate.WithName("Prepare sandbox image"), restate.WithMaxRetryAttempts(infrastructureAttempts))
}

func failureOutput(err error) []byte {
	output, _ := result.EncodeFailure(result.Failure{Message: err.Error()})
	return output
}

type signalValue struct {
	Name  string
	Value []byte
}

func (w *DurableAdapter) signal(ctx restate.WorkflowSharedContext, input signalValue) (restate.Void, error) {
	if err := validateIdempotency(input.Name, true); err != nil {
		return restate.Void{}, restate.ToTerminalError(err)
	}
	if err := validateSignal(input.Value); err != nil {
		return restate.Void{}, restate.ToTerminalError(err)
	}
	if err := resolvePromise(ctx, input.Name, &promiseOutcome{Value: input.Value}); err != nil {
		return restate.Void{}, restate.ToTerminalError(err)
	}
	return restate.Void{}, nil
}
