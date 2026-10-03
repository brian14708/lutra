package lutra

import (
	"bytes"
	"context"
	"errors"
)

func (w *Worker) ensureImage(ctx context.Context, imageKey []byte, executor Executor, req *EnvironmentExecution) (*Image, error) {
	if len(imageKey) != 32 {
		return nil, errors.New("invalid image key")
	}
	currentKey, err := executor.ImageKey(req.Spec)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(imageKey, currentKey) {
		return nil, errors.New("image recipe differs from registered environment")
	}
	return executor.Build(ctx, req)
}
