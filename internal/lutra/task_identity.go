package lutra

import (
	"context"

	"github.com/google/uuid"
)

// TaskIdentity is attached only to sandbox callbacks by the execution backend.
type TaskIdentity struct {
	RunID    uuid.UUID
	ActionID uuid.UUID
	Attempt  int32
}

type taskIdentityKey struct{}

func withTaskIdentity(ctx context.Context, task TaskIdentity) context.Context {
	return context.WithValue(ctx, taskIdentityKey{}, task)
}

func TaskIdentityFromContext(ctx context.Context) (TaskIdentity, bool) {
	task, ok := ctx.Value(taskIdentityKey{}).(TaskIdentity)
	return task, ok
}
