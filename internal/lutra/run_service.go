package lutra

import (
	"context"
	"errors"
	"uuid"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/auth"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/rpcutil"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/riverqueue/river"
)

// RunService implements workflow run and action operations.
type RunService struct {
	lutrav1connect.UnimplementedRunServiceHandler
	store  *auth.Store
	worker runWorker
}

// NewRunService creates a run service backed by store and worker.
func NewRunService(store *auth.Store, worker runWorker) *RunService {
	return &RunService{store: store, worker: worker}
}

// CreateRun starts a workflow run for a task.
func (s *RunService) CreateRun(ctx context.Context, req *connect.Request[lutrav1.CreateRunRequest]) (*connect.Response[lutrav1.CreateRunResponse], error) {
	projectID := req.Msg.GetProjectId()
	if err := s.store.Require(ctx, projectID, "run", "execute"); err != nil {
		return nil, err
	}
	task, err := s.store.Queries().GetTask(ctx, db.GetTaskParams{TaskID: rpcutil.UUID(req.Msg.GetTaskId()), ProjectID: rpcutil.UUID(projectID)})
	if err != nil {
		return nil, rpcutil.DB(err)
	}
	inputs, err := marshalBindings(req.Msg.GetInputs())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if int64(len(inputs)) > configuredLimit("LUTRA_MAX_INPUT_BYTES", 16<<20) {
		return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("run inputs exceed the configured size limit"))
	}
	runID, actionID := uuid.New(), uuid.New()
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := s.store.Queries().WithTx(tx)
	run, err := q.CreateRun(ctx, db.CreateRunParams{RunID: runID, ProjectID: rpcutil.UUID(projectID), IdempotencyKey: req.Msg.GetIdempotencyKey()})
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	// A retry with the same idempotency key returns the original run. Verify
	// that the retried request is identical before returning it.
	if existing, lookupErr := q.GetRootAction(ctx, run.RunID); lookupErr == nil {
		if existing.TaskID != task.TaskID || !equalJSON(existing.Inputs, inputs) {
			return nil, connect.NewError(connect.CodeAlreadyExists, errors.New("idempotency key was used for a different run"))
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, rpcutil.Internal(err)
		}
		result := runProto(run, existing.TaskID)
		result.RootAction = actionProto(existing)
		return connect.NewResponse(&lutrav1.CreateRunResponse{Run: result}), nil
	} else if !errors.Is(lookupErr, pgx.ErrNoRows) {
		return nil, rpcutil.Internal(lookupErr)
	}
	action, err := q.CreateAction(ctx, db.CreateActionParams{ActionID: actionID, ProjectID: rpcutil.UUID(projectID), RunID: run.RunID, ParentActionID: nil, OperationID: pgtype.Text{}, TaskID: task.TaskID, Inputs: inputs})
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	if err := s.worker.EnqueueTx(ctx, tx, projectID, run.RunID.String(), actionID.String(), river.QueueDefault); err != nil {
		return nil, rpcutil.Internal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, rpcutil.Internal(err)
	}
	result := runProto(run, action.TaskID)
	result.RootAction = actionProto(action)
	return connect.NewResponse(&lutrav1.CreateRunResponse{Run: result}), nil
}

// GetRun returns a workflow run.
func (s *RunService) GetRun(ctx context.Context, req *connect.Request[lutrav1.GetRunRequest]) (*connect.Response[lutrav1.GetRunResponse], error) {
	if err := s.store.Require(ctx, req.Msg.GetProjectId(), "run", "read"); err != nil {
		return nil, err
	}
	run, err := s.store.Queries().GetRun(ctx, db.GetRunParams{RunID: rpcutil.UUID(req.Msg.GetRunId()), ProjectID: rpcutil.UUID(req.Msg.GetProjectId())})
	if err != nil {
		return nil, rpcutil.DB(err)
	}
	root, err := s.store.Queries().GetRootAction(ctx, run.RunID)
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	rootAction, err := loadActionProto(ctx, s.store.Queries(), root)
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	result := runProto(run, root.TaskID)
	result.RootAction = rootAction
	return connect.NewResponse(&lutrav1.GetRunResponse{Run: result}), nil
}

// ListRuns returns workflow runs visible in a project.
func (s *RunService) ListRuns(ctx context.Context, req *connect.Request[lutrav1.ListRunsRequest]) (*connect.Response[lutrav1.ListRunsResponse], error) {
	if err := s.store.Require(ctx, req.Msg.GetProjectId(), "run", "read"); err != nil {
		return nil, err
	}
	runs, err := s.store.Queries().ListRuns(ctx, rpcutil.UUID(req.Msg.GetProjectId()))
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	result := make([]*lutrav1.Run, 0, len(runs))
	for _, run := range runs {
		result = append(result, runProto(run.LutraRun, run.RootTaskID))
	}
	return connect.NewResponse(&lutrav1.ListRunsResponse{Runs: result}), nil
}

// GetAction returns an action and its attempts.
func (s *RunService) GetAction(ctx context.Context, req *connect.Request[lutrav1.GetActionRequest]) (*connect.Response[lutrav1.GetActionResponse], error) {
	if err := s.store.Require(ctx, req.Msg.GetProjectId(), "action", "read"); err != nil {
		return nil, err
	}
	action, err := s.store.Queries().GetAction(ctx, db.GetActionParams{ActionID: rpcutil.UUID(req.Msg.GetActionId()), ProjectID: rpcutil.UUID(req.Msg.GetProjectId()), RunID: rpcutil.UUID(req.Msg.GetRunId())})
	if err != nil {
		return nil, rpcutil.DB(err)
	}
	result, err := loadActionProto(ctx, s.store.Queries(), action)
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	return connect.NewResponse(&lutrav1.GetActionResponse{Action: result}), nil
}

// ListActions returns the actions and attempts in a workflow run.
func (s *RunService) ListActions(ctx context.Context, req *connect.Request[lutrav1.ListActionsRequest]) (*connect.Response[lutrav1.ListActionsResponse], error) {
	if err := s.store.Require(ctx, req.Msg.GetProjectId(), "action", "read"); err != nil {
		return nil, err
	}
	runID := rpcutil.UUID(req.Msg.GetRunId())
	actions, err := s.store.Queries().ListActions(ctx, db.ListActionsParams{ProjectID: rpcutil.UUID(req.Msg.GetProjectId()), RunID: runID})
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	// One query for the whole run: a run with many children would otherwise
	// issue one attempt query per action.
	attempts, err := s.store.Queries().ListActionAttemptsForRun(ctx, runID)
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	attemptsByAction := make(map[uuid.UUID][]db.LutraActionAttempt, len(actions))
	for _, attempt := range attempts {
		attemptsByAction[attempt.ActionID] = append(attemptsByAction[attempt.ActionID], attempt)
	}
	result := make([]*lutrav1.Action, 0, len(actions))
	for _, action := range actions {
		result = append(result, actionProtoWithAttempts(action, attemptsByAction[action.ActionID]))
	}
	return connect.NewResponse(&lutrav1.ListActionsResponse{Actions: result}), nil
}

// CancelRun cancels a workflow run and its unfinished actions.
func (s *RunService) CancelRun(ctx context.Context, req *connect.Request[lutrav1.CancelRunRequest]) (*connect.Response[lutrav1.CancelRunResponse], error) {
	if err := s.store.Require(ctx, req.Msg.GetProjectId(), "run", "execute"); err != nil {
		return nil, err
	}
	run, err := s.store.Queries().GetRun(ctx, db.GetRunParams{RunID: rpcutil.UUID(req.Msg.GetRunId()), ProjectID: rpcutil.UUID(req.Msg.GetProjectId())})
	if err != nil {
		return nil, rpcutil.DB(err)
	}
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := s.store.Queries().WithTx(tx)
	if err := q.CancelActionsForRun(ctx, run.RunID); err != nil {
		return nil, rpcutil.Internal(err)
	}
	if err := q.CancelAttemptsForRun(ctx, run.RunID); err != nil {
		return nil, rpcutil.Internal(err)
	}
	runID, projectID := run.RunID, run.ProjectID
	run, err = q.CancelRunState(ctx, db.CancelRunStateParams{RunID: runID, ProjectID: projectID})
	if errors.Is(err, pgx.ErrNoRows) {
		run, err = q.GetRun(ctx, db.GetRunParams{RunID: runID, ProjectID: projectID})
	}
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, rpcutil.Internal(err)
	}
	s.worker.CancelRun(run.ProjectID.String(), run.RunID.String())
	root, err := s.store.Queries().GetRootAction(ctx, run.RunID)
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	return connect.NewResponse(&lutrav1.CancelRunResponse{Run: runProto(run, root.TaskID)}), nil
}
