package lutra

import (
	"context"

	"github.com/brian14708/lutra/internal/tasktree"
	"github.com/google/uuid"
)

// TaskIdentity identifies a task attempt and the run lease that owns it.
type TaskIdentity struct {
	RunID      uuid.UUID
	ActionID   uuid.UUID
	ClaimToken uuid.UUID
	Attempt    int32
}

// TaskIdentityFromContext returns the identity attached by the worker.
func TaskIdentityFromContext(ctx context.Context) (TaskIdentity, bool) {
	task, ok := taskContextFromContext(ctx)
	return task.TaskIdentity, ok
}

type taskContext struct {
	TaskIdentity
	coordinator *tasktree.Coordinator
	add         func(context.Context, uuid.UUID) error
}

type taskContextKey struct{}

func withTaskContext(ctx context.Context, task taskContext) context.Context {
	return context.WithValue(ctx, taskContextKey{}, task)
}

func taskContextFromContext(ctx context.Context) (taskContext, bool) {
	task, ok := ctx.Value(taskContextKey{}).(taskContext)
	return task, ok
}
