package lutra

import (
	"context"
	"errors"

	"github.com/brian14708/lutra/internal/cache"
	"github.com/brian14708/lutra/internal/tasktree"
)

func (d *runDriver) Acquire(ctx context.Context, key []byte) ([]byte, error, tasktree.CacheLease, error) {
	var digest [32]byte
	if len(key) != len(digest) {
		return nil, nil, nil, errors.New("invalid task cache key")
	}
	copy(digest[:], key)
	task, lease, err := d.worker.cache.Acquire(ctx, digest)
	if err != nil {
		return nil, nil, nil, err
	}
	if lease != nil {
		return nil, nil, taskLease{lease}, nil
	}
	if task.ErrorCode == "" {
		return task.OutputCBOR, nil, nil, nil
	}
	failure := &CacheableError{Code: task.ErrorCode, Details: task.ErrorDetailsCBOR}
	output, err := failure.output()
	if err != nil {
		return nil, nil, nil, err
	}
	return output, failure, nil, nil
}

type taskLease struct{ *cache.Lease }

func (l taskLease) Finish(ctx context.Context, output []byte, taskErr error) error {
	var failure *CacheableError
	if taskErr != nil && !errors.As(taskErr, &failure) {
		return l.Release(ctx)
	}
	if failure != nil {
		return l.Complete(ctx, cache.Result{ErrorCode: failure.Code, ErrorDetailsCBOR: failure.Details})
	}
	return l.Complete(ctx, cache.Result{OutputCBOR: output})
}
