package lutra

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// actionEnqueuer is the queue operation shared by run and worker RPCs.
type actionEnqueuer interface {
	EnqueueTx(ctx context.Context, tx pgx.Tx, projectID, runID, actionID, queue string) error
}

// runWorker is the worker surface needed by the public run service.
type runWorker interface {
	actionEnqueuer
	CancelRun(projectID, runID string)
}

// workerServiceRuntime is the capability-scoped surface needed by worker RPCs.
type workerServiceRuntime interface {
	actionEnqueuer
	readArtifact(ctx context.Context, projectID, artifactID string) ([]byte, error)
}
