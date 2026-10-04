package lutra

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"connectrpc.com/connect"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/runlog"
	"github.com/google/uuid"
)

const maxSourceSize = 64 << 20

func (w *RunWorker) openBundle(ctx context.Context, digest []byte) (io.ReadCloser, error) {
	url, err := w.presignBundle(ctx, digest)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		_ = response.Body.Close()
		return nil, fmt.Errorf("download source bundle: %s", response.Status)
	}
	return response.Body, nil
}

func (w *RunWorker) presignBundle(ctx context.Context, digest []byte) (string, error) {
	if w.Blobs == nil {
		return "", errors.New("blob client unavailable")
	}
	response, err := w.Blobs.GetDownload(ctx, connect.NewRequest(&lutrav1.GetDownloadRequest{Uri: sourceURI(digest)}))
	if err != nil {
		return "", err
	}
	return response.Msg.GetUrl(), nil
}

// executor returns the provider for an image name. Unconfigured providers
// fail explicitly until their adapters exist.
func (w *RunWorker) executor(name string) (Executor, error) {
	switch name {
	case containerTaskImage:
		runtime, err := containerRuntime()
		if err != nil {
			return nil, err
		}
		return &ContainerExecutor{OpenBundle: w.openBundle, Runtime: runtime}, nil
	}
	return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("%s executor is not configured", name))
}

func (w *RunWorker) executeEnvironment(ctx context.Context, image *Image, identity taskContext, task *EnvironmentExecution) ([]byte, error) {
	runID, actionID := uuid.MustParse(task.RunID), uuid.MustParse(task.ActionID)
	stderrReader, stderrWriter := io.Pipe()
	logDone := w.collectTaskLogs(runlog.TaskLogEvent{ActionID: actionID.String(), Attempt: task.Attempt, Phase: "task"}, runID, stderrReader)
	defer func() {
		_ = stderrWriter.Close()
		<-logDone
	}()
	task.Stderr = stderrWriter
	task.TaskAPIHandler = w.newTaskAPIHandler(identity)
	executor, err := w.executor(task.Provider)
	if err != nil {
		return nil, err
	}
	job, err := executor.Run(ctx, image, task)
	if err != nil {
		var config *ConfigError
		if errors.As(err, &config) {
			return nil, config
		}
		return nil, errors.New(task.Config.filter().String(err.Error()))
	}
	output, waitErr := job.Wait(ctx)
	if ctx.Err() != nil {
		_ = job.Kill(context.Background())
	}
	if waitErr != nil {
		var config *ConfigError
		var cacheable *CacheableError
		if !errors.As(waitErr, &config) && !errors.As(waitErr, &cacheable) {
			waitErr = errors.New(task.Config.filter().String(waitErr.Error()))
		}
	}
	return output, waitErr
}

func (w *RunWorker) newTaskAPIHandler(identity taskContext) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requestCtx := withTaskContext(request.Context(), identity)
		w.TaskAPIHandler.ServeHTTP(response, request.WithContext(requestCtx))
	})
}

func (w *RunWorker) collectTaskLogs(metadata runlog.TaskLogEvent, runID uuid.UUID, reader io.Reader) <-chan struct{} {
	actionID, attempt := metadata.ActionID, metadata.Attempt
	collectionID := uuid.New()
	done := make(chan struct{})
	go func() {
		defer close(done)
		lines := make(chan string)
		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			defer close(lines)
			input := bufio.NewReader(reader)
			for {
				line, err := input.ReadString('\n')
				if len(line) > 0 {
					if line[len(line)-1] == '\n' {
						line = line[:len(line)-1]
						if len(line) > 0 && line[len(line)-1] == '\r' {
							line = line[:len(line)-1]
						}
					}
					lines <- line
				}
				if err != nil {
					return
				}
			}
		}()
		batch := make([]*lutrav1.LogEntry, 0, 32)
		batchNumber := 0
		flush := func() {
			if len(batch) == 0 {
				return
			}
			batchNumber++
			appendID := fmt.Sprintf("task-log:%s:%s:%d", actionID, collectionID, batchNumber)
			appendCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if _, err := w.Logs.AppendEntries(appendCtx, runID, runlog.TaskLogStream, appendID, batch); err != nil {
				slog.Error("append task log", "action_id", actionID, "attempt", attempt, "error", err)
			}
			cancel()
			batch = batch[:0]
		}
		timer := time.NewTicker(100 * time.Millisecond)
		defer timer.Stop()
		for {
			select {
			case line, ok := <-lines:
				if !ok {
					flush()
					<-readDone
					return
				}
				entry := metadata
				entry.Type, entry.Source = "task.log.v1", "stderr"
				entry.Message, entry.Timestamp = line, time.Now().UTC().Format(time.RFC3339Nano)
				event, err := runlog.EncodeTaskLog(entry)
				if err != nil {
					slog.Error("encode task log", "action_id", actionID, "error", err)
					continue
				}
				batch = append(batch, &lutrav1.LogEntry{Key: []byte(actionID), ValueCbor: event})
				if len(batch) >= 32 {
					flush()
				}
			case <-timer.C:
				flush()
			}
		}
	}()
	return done
}
