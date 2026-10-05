package lutra

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/result"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	restate "github.com/restatedev/sdk-go"
)

// Restate cancels Run closures cooperatively, so stop active sandboxes from the
// committed cancellation without waiting for the closure to finish. Successful
// workflows must still replay until Restate records their final result.
func (w *DurableAdapter) actionContext(parent context.Context, id uuid.UUID) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			status, err := db.New(w.DB).GetRootStatus(ctx, id)
			if err == nil && status == db.LutraTaskActionStatusCanceled {
				cancel()
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return ctx, cancel
}

func (w *DurableAdapter) begin(ctx context.Context, id uuid.UUID) (int32, error) {
	tx, err := w.DB.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	q := db.New(tx)
	row, err := q.ReadTaskAction(ctx, id)
	if err != nil {
		return 0, err
	}
	if _, err := q.LockRun(ctx, row.RunID); err != nil {
		return 0, err
	}
	attempt, err := q.StartTaskAttempt(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, restate.TerminalErrorf("action is no longer active")
	}
	if err != nil {
		return 0, err
	}
	if err := w.Logs.AppendActionStatus(ctx, tx, id, false); err != nil {
		return 0, err
	}
	if row.CallerActionID == nil {
		if err := w.Logs.AppendStatus(ctx, tx, row.RunID); err != nil {
			return 0, err
		}
	}
	return attempt, tx.Commit(ctx)
}

func (w *DurableAdapter) project(ctx context.Context, id uuid.UUID, status db.LutraTaskActionStatus, output []byte, hit bool) error {
	tx, err := w.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	q := db.New(tx)
	row, err := q.ReadTaskAction(ctx, id)
	if err != nil {
		return err
	}
	if _, err := q.LockRun(ctx, row.RunID); err != nil {
		return err
	}
	rows, err := q.ProjectTaskAction(ctx, db.ProjectTaskActionParams{ActionID: id, Status: status, ResultCbor: output})
	if err != nil {
		return err
	}
	if rows == 0 {
		_ = tx.Rollback(ctx)
		if terminal(string(status)) {
			return w.cancelCanceled(ctx, row.RunID)
		}
		return nil
	}
	if err := w.Logs.AppendActionStatus(ctx, tx, id, hit); err != nil {
		return err
	}
	if row.CallerActionID == nil {
		if err := w.Logs.AppendStatus(ctx, tx, row.RunID); err != nil {
			return err
		}
	}
	if terminal(string(status)) {
		canceled, err := q.CancelDescendants(ctx, db.CancelDescendantsParams{ParentID: id, ResultCbor: failureOutput(errors.New("parent completed"))})
		if err != nil {
			return err
		}
		for _, child := range canceled {
			if err := w.Logs.AppendActionStatus(ctx, tx, child, false); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if terminal(string(status)) {
		return w.cancelCanceled(ctx, row.RunID)
	}
	return nil
}

func (w *DurableAdapter) cancelCanceled(ctx context.Context, runID uuid.UUID) error {
	actions, err := db.New(w.DB).CanceledActions(ctx, runID)
	if err != nil {
		return err
	}
	var failures []error
	for _, action := range actions {
		if err := w.Dispatch.cancelAction(ctx, action); err != nil {
			failures = append(failures, fmt.Errorf("cancel action %s: %w", action, err))
		}
	}
	return errors.Join(failures...)
}

func (w *DurableAdapter) finish(ctx context.Context, id uuid.UUID, out executionResult) error {
	_, failed, err := result.DecodeFailure(out.Output)
	if err != nil {
		return err
	}
	status := db.LutraTaskActionStatusSucceeded
	if failed {
		status = db.LutraTaskActionStatusFailed
	}
	return w.project(ctx, id, status, out.Output, out.CacheHit)
}
