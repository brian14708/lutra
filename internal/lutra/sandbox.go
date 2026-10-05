package lutra

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
)

const maxSourceSize = 64 << 20

func (w *SandboxRuntime) openBundle(ctx context.Context, digest []byte) (io.ReadCloser, error) {
	if w.Blobs == nil {
		return nil, errors.New("blob client unavailable")
	}
	response, err := w.Blobs.GetDownload(ctx, connect.NewRequest(&lutrav1.GetDownloadRequest{Uri: sourceURI(digest)}))
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, response.Msg.GetUrl(), nil)
	if err != nil {
		return nil, err
	}
	download, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	if download.StatusCode != http.StatusOK {
		_ = download.Body.Close()
		return nil, fmt.Errorf("download source bundle: %s", download.Status)
	}
	return download.Body, nil
}

func (w *SandboxRuntime) executor(name string) (Executor, error) {
	if name != containerTaskImage {
		return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("%s executor is not configured", name))
	}
	runtime, err := w.runtime()
	if err != nil {
		return nil, err
	}
	return &ContainerExecutor{OpenBundle: w.openBundle, Runtime: runtime}, nil
}

func (w *SandboxRuntime) prepare(ctx context.Context, task *EnvironmentExecution, key []byte) (*Image, error) {
	executor, err := w.executor(task.Provider)
	if err != nil {
		return nil, err
	}
	runtime, err := w.runtime()
	if err != nil {
		return nil, err
	}
	sink := w.newLogSink(task, runtime, imageTag(key))
	defer sink.Close()
	task.Output = sink
	return ensureImage(ctx, key, executor, task)
}

func (w *SandboxRuntime) executeEnvironment(ctx context.Context, image *Image, identity TaskIdentity, task *EnvironmentExecution, handler http.Handler) ([]byte, error) {
	sink := w.newLogSink(task, "", "")
	defer sink.Close()
	task.Output, task.TaskAPIHandler = sink, handler
	executor, err := w.executor(task.Provider)
	if err != nil {
		return nil, err
	}
	job, err := executor.Run(ctx, image, task)
	if err != nil {
		return nil, task.Config.redactError(err)
	}
	output, waitErr := job.Wait(ctx)
	if ctx.Err() != nil {
		_ = job.Kill(context.Background())
	}
	if waitErr != nil {
		waitErr = task.Config.redactError(waitErr)
	}
	return output, waitErr
}

func (w *SandboxRuntime) newTaskAPIHandler(identity TaskIdentity) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		w.TaskAPIHandler.ServeHTTP(response, request.WithContext(withTaskIdentity(request.Context(), identity)))
	})
}
