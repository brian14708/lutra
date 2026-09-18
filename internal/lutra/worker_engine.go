package lutra

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
	"uuid"

	"github.com/brian14708/lutra/internal/auth"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/rpcutil"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const childQueueName = "lutra_children"

// WorkerEngine schedules action execution on the durable River queue backed by
// the same PostgreSQL database. Each action is reloaded from PostgreSQL before
// execution, so duplicate delivery is fenced by ClaimAction and is safe under
// at-least-once scheduling.
type WorkerEngine struct {
	store           *auth.Store
	workers         int
	executor        *pythonExecutor
	river           *river.Client[pgx.Tx]
	activeMu        sync.Mutex
	active          map[uuid.UUID]context.CancelFunc
	tokenIssuer     func(projectID, runID, actionID string, attempt int32, fencingToken string) (string, time.Time)
	reconcileMu     sync.Mutex
	reconcileCancel context.CancelFunc
	reconcileDone   chan struct{}
}

// ExecuteActionArgs is the durable River payload.
type ExecuteActionArgs struct {
	ProjectID   string `json:"project_id"`
	RunID       string `json:"run_id"`
	ActionID    string `json:"action_id"`
	TraceParent string `json:"traceparent,omitempty"`
	TraceState  string `json:"tracestate,omitempty"`
}

// Kind returns the River job kind for action execution.
func (ExecuteActionArgs) Kind() string { return "execute_action" }

type executeActionWorker struct {
	river.WorkerDefaults[ExecuteActionArgs]
	engine *WorkerEngine
}

func (w *executeActionWorker) Work(ctx context.Context, job *river.Job[ExecuteActionArgs]) error {
	ctx = extractWorkerTraceContext(ctx, job.Args.TraceParent, job.Args.TraceState)
	return w.engine.execute(ctx, job.Args.ProjectID, job.Args.RunID, job.Args.ActionID, job.ID, job.Queue)
}

var (
	workerTracer     = otel.Tracer("github.com/brian14708/lutra/worker")
	workerMeter      = otel.Meter("github.com/brian14708/lutra/worker")
	workerExecutions metric.Int64Counter
	workerDuration   metric.Float64Histogram
)

func init() {
	workerExecutions, _ = workerMeter.Int64Counter("lutra.worker.executions")
	workerDuration, _ = workerMeter.Float64Histogram("lutra.worker.duration", metric.WithUnit("s"))
}

// NewWorkerEngine creates a durable action worker with the requested concurrency.
func NewWorkerEngine(store *auth.Store, concurrency int) *WorkerEngine {
	if concurrency < 1 {
		concurrency = 1
	}
	return &WorkerEngine{
		store:    store,
		workers:  concurrency,
		executor: newPythonExecutor(store),
		active:   make(map[uuid.UUID]context.CancelFunc),
	}
}

// SetAttemptTokenIssuer configures the issuer used for worker attempt tokens.
func (w *WorkerEngine) SetAttemptTokenIssuer(issuer func(projectID, runID, actionID string, attempt int32, fencingToken string) (string, time.Time)) {
	w.tokenIssuer = issuer
}

// ConfigureRiver attaches the durable queue to the same PostgreSQL database.
// River's own tables are explicitly kept in the lutra schema alongside the
// workflow state tables.
func (w *WorkerEngine) ConfigureRiver(pool *pgxpool.Pool, logger *slog.Logger) error {
	workers := river.NewWorkers()
	river.AddWorker(workers, &executeActionWorker{engine: w})
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Logger: logger,
		Schema: "lutra",
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: w.workers},
			childQueueName:     {MaxWorkers: int(configuredLimit("LUTRA_CHILD_WORKERS", int64(max(4, w.workers*4))))},
		},
		Workers: workers,
		// The Python executor applies the configured task timeout. River must not
		// cancel the database completion transaction before that timeout.
		JobTimeout: -1,
	})
	if err != nil {
		return err
	}
	w.river = client
	return nil
}

// Start begins processing queued actions and recovery work.
func (w *WorkerEngine) Start(ctx context.Context) error {
	if err := w.river.Start(ctx); err != nil {
		return err
	}
	if w.store == nil {
		return nil
	}
	if err := w.reconcile(ctx); err != nil {
		return err
	}
	w.reconcileMu.Lock()
	if w.reconcileCancel == nil {
		reconcileCtx, cancel := context.WithCancel(ctx)
		w.reconcileCancel = cancel
		w.reconcileDone = make(chan struct{})
		go w.reconciliationLoop(reconcileCtx, w.reconcileDone)
	}
	w.reconcileMu.Unlock()
	return nil
}

func (w *WorkerEngine) reconciliationLoop(ctx context.Context, done chan struct{}) {
	defer close(done)
	interval := time.Duration(configuredLimit("LUTRA_RECOVERY_INTERVAL_SECONDS", 30)) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.reconcile(ctx); err != nil {
				slog.Default().ErrorContext(ctx, "worker reconciliation failed", "error", err)
			}
		}
	}
}

func (w *WorkerEngine) reconcile(ctx context.Context) error {
	if w.store == nil {
		return nil
	}
	orphaned, err := w.store.Queries().RecoverOrphanedActions(ctx)
	if err != nil {
		return err
	}
	w.requeue(orphaned)
	expired, err := w.store.Queries().RecoverExpiredActions(ctx)
	if err != nil {
		return err
	}
	w.requeue(expired)
	return nil
}

// requeue returns recovered actions to their queue; children go to the
// higher-concurrency child queue.
func (w *WorkerEngine) requeue(actions []db.LutraAction) {
	for _, action := range actions {
		if action.State != db.LutraActionStateReady {
			continue
		}
		queue := river.QueueDefault
		if action.ParentActionID != nil {
			queue = childQueueName
		}
		w.Enqueue(action.ProjectID.String(), action.RunID.String(), action.ActionID.String(), queue)
	}
}

// Stop stops action processing and waits for the reconciliation loop.
func (w *WorkerEngine) Stop(ctx context.Context) error {
	w.reconcileMu.Lock()
	cancel := w.reconcileCancel
	done := w.reconcileDone
	w.reconcileCancel = nil
	w.reconcileDone = nil
	w.reconcileMu.Unlock()
	if cancel != nil {
		cancel()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return w.river.Stop(ctx)
}

// Enqueue schedules an action asynchronously.
func (w *WorkerEngine) Enqueue(projectID, runID, actionID, queue string) {
	ctx, span := workerTracer.Start(context.Background(), "lutra.worker.enqueue", trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(
		attribute.String("lutra.project_id", projectID),
		attribute.String("lutra.run_id", runID),
		attribute.String("lutra.action_id", actionID),
		attribute.String("messaging.system", "river"),
		attribute.String("messaging.operation.type", "send"),
		attribute.String("messaging.destination.name", queue),
	))
	defer span.End()
	args := durableActionArgs(ctx, projectID, runID, actionID)
	_, err := w.river.Insert(ctx, args, &river.InsertOpts{Queue: queue})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		slog.Default().Error("worker action enqueue failed", "action_id", actionID, "error", err)
	}
}

// EnqueueTx inserts a job on the same transaction that creates its action.
// River's visibility rules ensure a worker cannot observe a half-created
// action.
func (w *WorkerEngine) EnqueueTx(ctx context.Context, tx pgx.Tx, projectID, runID, actionID, queue string) error {
	ctx, span := workerTracer.Start(ctx, "lutra.worker.enqueue", trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(
		attribute.String("lutra.project_id", projectID),
		attribute.String("lutra.run_id", runID),
		attribute.String("lutra.action_id", actionID),
		attribute.String("messaging.system", "river"),
		attribute.String("messaging.operation.type", "send"),
		attribute.String("messaging.destination.name", queue),
	))
	defer span.End()
	args := durableActionArgs(ctx, projectID, runID, actionID)
	_, err := w.river.InsertTx(ctx, tx, args, &river.InsertOpts{Queue: queue})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

func durableActionArgs(ctx context.Context, projectID, runID, actionID string) ExecuteActionArgs {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return ExecuteActionArgs{
		ProjectID: projectID, RunID: runID, ActionID: actionID,
		TraceParent: carrier.Get("traceparent"), TraceState: carrier.Get("tracestate"),
	}
}

func extractWorkerTraceContext(ctx context.Context, traceParent, traceState string) context.Context {
	carrier := propagation.MapCarrier{}
	if traceParent != "" {
		carrier.Set("traceparent", traceParent)
	}
	if traceState != "" {
		carrier.Set("tracestate", traceState)
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

func (w *WorkerEngine) execute(ctx context.Context, projectID, runID, actionID string, jobID int64, queue string) (err error) {
	ctx, span := workerTracer.Start(ctx, "lutra.worker.execute", trace.WithAttributes(
		attribute.String("lutra.project_id", projectID),
		attribute.String("lutra.run_id", runID),
		attribute.String("lutra.action_id", actionID),
		attribute.String("lutra.component", "worker"),
		attribute.String("messaging.system", "river"),
		attribute.String("messaging.operation.type", "process"),
		attribute.String("messaging.destination.name", queue),
		attribute.Int64("messaging.message.id", jobID),
	), trace.WithSpanKind(trace.SpanKindConsumer))
	started := time.Now()
	defer func() {
		workerDuration.Record(ctx, time.Since(started).Seconds())
		workerExecutions.Add(ctx, 1, metric.WithAttributes(attribute.String("lutra.status", workerStatus(err))))
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	claimTx, err := w.store.Begin(workCtx)
	if err != nil {
		return err
	}
	claimQueries := w.store.Queries().WithTx(claimTx)
	action, err := claimQueries.ClaimAction(workCtx, db.ClaimActionParams{ActionID: rpcutil.UUID(actionID), ProjectID: rpcutil.UUID(projectID)})
	if err != nil {
		_ = claimTx.Rollback(workCtx)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	if _, err := claimQueries.MarkRunRunning(workCtx, db.MarkRunRunningParams{RunID: action.RunID, ProjectID: action.ProjectID}); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		_ = claimTx.Rollback(workCtx)
		return err
	} else if errors.Is(err, pgx.ErrNoRows) {
		_ = claimTx.Rollback(workCtx)
		return nil
	}
	span.SetAttributes(
		attribute.String("lutra.task_id", action.TaskID.String()),
		attribute.Int("lutra.attempt", int(action.AttemptCount)),
	)
	w.activeMu.Lock()
	w.active[action.ActionID] = cancel
	w.activeMu.Unlock()
	defer func() {
		cancel()
		w.activeMu.Lock()
		delete(w.active, action.ActionID)
		w.activeMu.Unlock()
	}()
	fencing := uuid.New()
	_, err = claimQueries.CreateActionAttempt(workCtx, db.CreateActionAttemptParams{ActionID: action.ActionID, Attempt: action.AttemptCount, LeaseOwner: "local", LeaseExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(10 * time.Minute), Valid: true}, FencingToken: fencing})
	if err != nil {
		_ = claimTx.Rollback(workCtx)
		return err
	}
	attemptOpen := true
	defer func() {
		if !attemptOpen {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		cleanupTx, cleanupErr := w.store.Begin(cleanupCtx)
		if cleanupErr == nil {
			cleanupQueries := w.store.Queries().WithTx(cleanupTx)
			_, cleanupErr = cleanupQueries.AbandonActionAttempt(cleanupCtx, db.AbandonActionAttemptParams{
				ActionID: action.ActionID, AttemptCount: action.AttemptCount, FencingToken: fencing,
				FailureMessage: "worker execution did not complete",
			})
			if cleanupErr == nil {
				cleanupErr = cleanupTx.Commit(cleanupCtx)
			} else {
				_ = cleanupTx.Rollback(cleanupCtx)
			}
		}
		if cleanupErr != nil {
			slog.Default().ErrorContext(workCtx, "worker action cleanup failed", "action_id", action.ActionID, "error", cleanupErr)
		}
	}()
	if err := claimTx.Commit(workCtx); err != nil {
		return err
	}
	var token string
	var tokenExpires time.Time
	if w.tokenIssuer != nil {
		token, tokenExpires = w.tokenIssuer(action.ProjectID.String(), action.RunID.String(), action.ActionID.String(), action.AttemptCount, fencing.String())
	}
	message, state, storedOutputs := w.executor.execute(workCtx, action, token, tokenExpires, fencing.String())
	outputs := []byte(`[]`)
	if storedOutputs != nil {
		outputs = storedOutputs
	}
	finishTx, err := w.store.Begin(workCtx)
	if err != nil {
		return err
	}
	finishQueries := w.store.Queries().WithTx(finishTx)
	_, finishErr := finishQueries.FinishAction(workCtx, db.FinishActionParams{ActionID: action.ActionID, ProjectID: action.ProjectID, AttemptCount: action.AttemptCount, State: state, Outputs: outputs, FailureMessage: message, FencingToken: fencing})
	if finishErr != nil {
		_ = finishTx.Rollback(workCtx)
		return finishErr
	}
	attemptState := db.LutraAttemptState(state)
	if state == db.LutraActionStateTimedOut {
		attemptState = db.LutraAttemptStateFailed
	}
	if rows, err := finishQueries.FinishActionAttempt(workCtx, db.FinishActionAttemptParams{ActionID: action.ActionID, Attempt: action.AttemptCount, FencingToken: fencing, State: attemptState, FailureMessage: message}); err != nil {
		_ = finishTx.Rollback(workCtx)
		return err
	} else if rows != 1 {
		_ = finishTx.Rollback(workCtx)
		return errors.New("action attempt was not open while finishing action")
	}
	if err := finalizeRunQueries(workCtx, finishQueries, action.RunID, action.ProjectID); err != nil {
		_ = finishTx.Rollback(workCtx)
		return err
	}
	if err := finishTx.Commit(workCtx); err != nil {
		return err
	}
	attemptOpen = false
	return nil
}

func workerStatus(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}

// CancelRun interrupts active workers for a workflow run.
func (w *WorkerEngine) CancelRun(projectID, runID string) {
	if w.store == nil {
		return
	}
	actions, err := w.store.Queries().ListActions(context.Background(), db.ListActionsParams{ProjectID: rpcutil.UUID(projectID), RunID: rpcutil.UUID(runID)})
	if err != nil {
		return
	}
	w.activeMu.Lock()
	defer w.activeMu.Unlock()
	for _, action := range actions {
		if cancel, ok := w.active[action.ActionID]; ok {
			cancel()
		}
	}
}

// readArtifact keeps artifact access behind WorkerEngine for the worker RPC
// service while the implementation lives with the Python executor.
func (w *WorkerEngine) readArtifact(ctx context.Context, projectID, artifactID string) ([]byte, error) {
	return w.executor.readArtifact(ctx, projectID, artifactID)
}

func finalizeRunQueries(ctx context.Context, queries *db.Queries, runID, projectID uuid.UUID) error {
	actions, err := queries.ListActions(ctx, db.ListActionsParams{ProjectID: projectID, RunID: runID})
	if err != nil {
		return err
	}
	if len(actions) == 0 {
		return nil
	}
	pending := false
	hasFailure := false
	hasCancellation := false
	for _, action := range actions {
		switch action.State {
		case db.LutraActionStateFailed, db.LutraActionStateTimedOut:
			hasFailure = true
		case db.LutraActionStateCanceled:
			hasCancellation = true
		case db.LutraActionStateSucceeded:
		default:
			pending = true
		}
	}
	// Do not expose a terminal run while sibling actions are still active. A
	// failed child may be accompanied by other children that are still running.
	if pending {
		return nil
	}
	state := db.LutraRunStateSucceeded
	if hasFailure {
		state = db.LutraRunStateFailed
	} else if hasCancellation {
		state = db.LutraRunStateCancelled
	}
	_, err = queries.UpdateRunState(ctx, db.UpdateRunStateParams{RunID: runID, ProjectID: projectID, State: state})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	return err
}
