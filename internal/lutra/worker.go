package lutra

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"connectrpc.com/connect"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/blob"
	"github.com/brian14708/lutra/internal/cache"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/runlog"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
)

const maxSourceSize = 64 << 20

// Worker provides sandbox, image and log adapters for run executors.
type Worker struct {
	DB             *pgxpool.Pool
	TaskAPIHandler http.Handler
	Store          *minio.Core
	Bucket         string
	Logs           runlog.Service
	queries        *db.Queries
	Cache          *cache.Service
}

func (w *Worker) openBundle(ctx context.Context, digest []byte) (io.ReadCloser, error) {
	if w.Store == nil || w.Bucket == "" {
		return nil, errors.New("source blob store unavailable")
	}
	record, err := w.queries.GetBlobBySHA256(ctx, digest)
	if err != nil {
		return nil, err
	}
	reader, _, _, err := w.Store.GetObject(ctx, w.Bucket, blob.ObjectKey(record.ObjectKey), minio.GetObjectOptions{})
	return reader, err
}

func (w *Worker) readVerified(ctx context.Context, objectID uuid.UUID, digest []byte, maxSize int64) ([]byte, error) {
	reader, _, _, err := w.Store.GetObject(ctx, w.Bucket, blob.ObjectKey(objectID), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	archive, err := io.ReadAll(io.LimitReader(reader, maxSize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(archive)) > maxSize {
		return nil, errors.New("blob exceeds size limit")
	}
	hash := sha256.Sum256(archive)
	if !bytes.Equal(hash[:], digest) {
		return nil, errors.New("blob checksum mismatch")
	}
	return archive, nil
}

// executor returns the provider for an image name. Unconfigured providers
// fail explicitly until their adapters exist.
func (w *Worker) executor(name string) (Executor, error) {
	switch name {
	case localTaskImage:
		return &LocalExecutor{StoreArtifact: w.storeArtifact, LoadArtifact: w.loadArtifact, OpenBundle: w.openBundle, RuntimeVersion: os.Getenv("LUTRA_PYTHON_RUNTIME_VERSION")}, nil
	case "docker":
		return &DockerExecutor{OpenBundle: w.openBundle}, nil
	}
	return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("%s executor is not configured", name))
}

func (w *Worker) executeEnvironment(ctx context.Context, image *Image, identity taskContext, task *EnvironmentExecution) ([]byte, error) {
	runID, actionID := uuid.MustParse(task.RunID), uuid.MustParse(task.ActionID)
	stderrReader, stderrWriter := io.Pipe()
	logDone := w.collectTaskLogs(runID, actionID, task.Attempt, stderrReader)
	defer func() {
		_ = stderrWriter.Close()
		<-logDone
	}()
	task.Stderr = stderrWriter
	task.TaskAPIHandler = http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case lutrav1connect.LutraServiceCreateTaskActionProcedure,
			lutrav1connect.LutraServiceGetTaskActionProcedure,
			lutrav1connect.LogServiceAppendProcedure,
			lutrav1connect.LogServiceReadProcedure,
			lutrav1connect.BlobServiceCreateUploadProcedure,
			lutrav1connect.BlobServicePresignPartProcedure,
			lutrav1connect.BlobServiceCompleteUploadProcedure,
			lutrav1connect.BlobServiceAbortUploadProcedure,
			lutrav1connect.BlobServiceGetDownloadProcedure:
		default:
			response.Header().Set("Content-Type", "application/json")
			response.WriteHeader(http.StatusForbidden)
			_, _ = response.Write([]byte(`{"code":"permission_denied","message":"procedure is unavailable to task"}`))
			return
		}
		requestCtx := context.WithValue(request.Context(), taskContextKey{}, identity)
		requestCtx = runlog.WithCheckpointScope(requestCtx, runID, actionID)
		w.TaskAPIHandler.ServeHTTP(response, request.WithContext(requestCtx))
	})
	executor, err := w.executor(task.Provider)
	if err != nil {
		return nil, err
	}
	job, err := executor.Run(ctx, image, task)
	if err != nil {
		return nil, err
	}
	output, waitErr := job.Wait(ctx)
	if ctx.Err() != nil {
		_ = job.Kill(context.Background())
	}
	return output, waitErr
}

func (w *Worker) collectTaskLogs(runID, actionID uuid.UUID, attempt int32, reader io.Reader) <-chan struct{} {
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
			appendID := fmt.Sprintf("task-log:%s:%d:%d", actionID, attempt, batchNumber)
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
				event, err := runlog.EncodeTaskLog(runlog.TaskLogEvent{Type: "task.log.v1", Source: "stderr", Message: line, Timestamp: time.Now().UTC().Format(time.RFC3339Nano), ActionID: actionID.String(), Attempt: attempt})
				if err != nil {
					slog.Error("encode task log", "action_id", actionID, "error", err)
					continue
				}
				batch = append(batch, &lutrav1.LogEntry{Key: []byte(actionID.String()), ValueCbor: event})
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
