package lutra

import (
	"context"
	"errors"

	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/graphexec"
	"github.com/brian14708/lutra/internal/runlog"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrLeaseLost = errors.New("run lease lost")

// graphStore is the narrow SQL adapter used by the in-process graph scheduler.
// It never reads scheduling state. The executor owns that state in memory.
type graphStore struct {
	pool *pgxpool.Pool
	logs runlog.Service
}

func graphStatus(s graphexec.State) db.LutraTaskActionStatus {
	switch s {
	case graphexec.Running:
		return db.LutraTaskActionStatusRunning
	case graphexec.Waiting:
		return db.LutraTaskActionStatusWaiting
	case graphexec.Done:
		return db.LutraTaskActionStatusSucceeded
	case graphexec.Failed:
		return db.LutraTaskActionStatusFailed
	case graphexec.Canceled:
		return db.LutraTaskActionStatusCanceled
	default:
		return db.LutraTaskActionStatusQueued
	}
}

func (s graphStore) Transition(ctx context.Context, t graphexec.Transition) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	q := db.New(tx)
	run, err := q.LockRun(ctx, t.RunID)
	if err != nil {
		return err
	}
	var next pgtype.Timestamptz
	if !t.NextAttemptAt.IsZero() {
		next = pgtype.Timestamptz{Time: t.NextAttemptAt, Valid: true}
	}
	rows, err := q.TransitionRunTaskAction(ctx, db.TransitionRunTaskActionParams{
		Status: graphStatus(t.State), Attempt: t.Attempt, Failures: t.Failures,
		OutputCbor: t.Output, Error: t.Error, NextAttemptAt: next,
		ActionID: t.NodeID, RunID: t.RunID, ClaimToken: t.ClaimToken,
		ExpectedAttempt: t.ExpectedAttempt,
	})
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrLeaseLost
	}
	if err := s.logs.AppendActionStatus(ctx, tx, t.NodeID, t.CacheHit); err != nil {
		return err
	}
	if run.RootActionID != nil && *run.RootActionID == t.NodeID {
		if err := s.logs.AppendStatus(ctx, tx, t.RunID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
