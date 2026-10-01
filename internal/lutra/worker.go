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
	"sync"
	"time"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/blob"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/runlog"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"google.golang.org/protobuf/proto"
)

type activeRun struct {
	runID    uuid.UUID
	actionID uuid.UUID
	ctx      context.Context
	cancel   context.CancelFunc
	token    uuid.UUID
	mu       sync.Mutex
	slotHeld bool
}

const (
	leaseDuration = 30 * time.Second
	leaseRenewal  = 10 * time.Second
	maxSourceSize = 64 << 20
)

// Worker claims persistent task actions and executes each attempt in a fresh bundle directory.
type Worker struct {
	DB             *pgxpool.Pool
	TaskAPIHandler http.Handler
	Store          *minio.Core
	Bucket         string
	Capacity       int
	Logs           runlog.Service
	slots          chan struct{}
	queries        *db.Queries
	mu             sync.Mutex
	active         map[uuid.UUID]*activeRun
}

func (w *Worker) Start(ctx context.Context) {
	capacity := w.Capacity
	if capacity <= 0 {
		capacity = 4
	}
	w.slots = make(chan struct{}, capacity)
	for range capacity {
		w.slots <- struct{}{}
	}
	w.active = make(map[uuid.UUID]*activeRun)
	w.queries = db.New(w.DB)
	go w.loop(ctx)
}

func (w *Worker) loop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.slots:
		}
		actionID, runID, token, attempt, err := w.claim(ctx)
		if err != nil || actionID == uuid.Nil {
			if err != nil && ctx.Err() == nil {
				slog.Error("claim run", "error", err)
			}
			w.slots <- struct{}{}
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		runCtx, cancel := context.WithCancel(ctx)
		active := &activeRun{ctx: runCtx, cancel: cancel, token: token, slotHeld: true, runID: runID, actionID: actionID}
		w.mu.Lock()
		w.active[actionID] = active
		w.mu.Unlock()
		go w.execute(runCtx, actionID, runID, attempt, active)
	}
}

func (w *Worker) claim(ctx context.Context) (uuid.UUID, uuid.UUID, uuid.UUID, int32, error) {
	token := uuid.New()
	tx, err := w.DB.Begin(ctx)
	if err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	claimed, err := db.New(tx).ClaimTaskAction(ctx, db.ClaimTaskActionParams{
		ClaimToken: token, LeaseSeconds: int32(leaseDuration / time.Second),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, uuid.Nil, uuid.Nil, 0, nil
	}
	if err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, 0, err
	}
	if claimed.CallerActionID == nil && claimed.PreviousStatus != db.LutraTaskActionStatusRunning {
		if err := w.Logs.AppendStatus(ctx, tx, claimed.RunID); err != nil {
			return uuid.Nil, uuid.Nil, uuid.Nil, 0, err
		}
	}
	if err := w.Logs.AppendActionStatus(ctx, tx, claimed.ID); err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, 0, err
	}
	return claimed.ID, claimed.RunID, token, claimed.Attempts, tx.Commit(ctx)
}

func (w *Worker) cancelRun(runID uuid.UUID) {
	w.mu.Lock()
	active := make([]*activeRun, 0)
	for _, candidate := range w.active {
		if candidate.runID == runID {
			active = append(active, candidate)
		}
	}
	w.mu.Unlock()
	for _, candidate := range active {
		candidate.cancel()
	}
}

func (w *Worker) cancelDescendants(runID, rootID uuid.UUID) {
	w.mu.Lock()
	active := make([]*activeRun, 0)
	for _, candidate := range w.active {
		if candidate.runID == runID && candidate.actionID != rootID {
			active = append(active, candidate)
		}
	}
	w.mu.Unlock()
	for _, candidate := range active {
		candidate.cancel()
	}
}

func (w *Worker) execute(ctx context.Context, actionID, runID uuid.UUID, attempt int32, active *activeRun) {
	stopRenewal := make(chan struct{})
	go w.renewLease(ctx, actionID, active, stopRenewal)
	defer func() {
		close(stopRenewal)
		active.cancel()
		active.mu.Lock()
		if active.slotHeld {
			w.slots <- struct{}{}
		}
		active.mu.Unlock()
		w.mu.Lock()
		if w.active[actionID] == active {
			delete(w.active, actionID)
		}
		w.mu.Unlock()
	}()
	claimed, err := w.queries.LoadClaimedTaskAction(ctx, db.LoadClaimedTaskActionParams{ActionID: actionID, ClaimToken: active.token})
	if err == nil {
		var task *EnvironmentExecution
		task, err = newExecution(claimed, runID, actionID, attempt)
		if err == nil {
			var output []byte
			output, err = w.executeEnvironment(ctx, claimed.ImageKey, active.token, task)
			if finishErr := w.finish(actionID, runID, active.token, attempt, output, err); finishErr != nil {
				slog.Error("finish action", "action_id", actionID, "error", finishErr)
			}
			return
		}
	}
	if finishErr := w.finish(actionID, runID, active.token, attempt, nil, err); finishErr != nil {
		slog.Error("finish action", "action_id", actionID, "error", finishErr)
	}
}

func newExecution(claimed db.LoadClaimedTaskActionRow, runID, actionID uuid.UUID, attempt int32) (*EnvironmentExecution, error) {
	task := &EnvironmentExecution{
		Environment:  &lutrav1.EnvironmentIdentifier{NamespaceId: claimed.NamespaceID.String(), Name: claimed.EnvironmentName, Version: claimed.Version},
		EntrypointID: uint32(claimed.EntrypointID),
		Provider:     claimed.Provider,
		Spec:         &lutrav1.EnvironmentSpec{},
		Input:        claimed.InputCbor,
		RunID:        runID.String(),
		ActionID:     actionID.String(),
		Attempt:      attempt,
	}
	if err := proto.Unmarshal(claimed.Spec, task.Spec); err != nil {
		return nil, err
	}
	task.Environments = append([]*lutrav1.EnvironmentIdentifier{task.Environment}, task.Spec.Dependencies...)
	return task, nil
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

func (w *Worker) renewLease(ctx context.Context, actionID uuid.UUID, active *activeRun, stop <-chan struct{}) {
	ticker := time.NewTicker(leaseRenewal)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			rows, err := w.queries.RenewTaskActionLease(renewCtx, db.RenewTaskActionLeaseParams{
				LeaseSeconds: int32(leaseDuration / time.Second), ActionID: actionID, ClaimToken: active.token,
			})
			cancel()
			if err != nil || rows != 1 {
				if err != nil && ctx.Err() == nil {
					slog.Error("renew action lease", "action_id", actionID, "error", err)
				}
				active.cancel()
				return
			}
		}
	}
}

func (w *Worker) finish(actionID, runID, token uuid.UUID, attempt int32, output []byte, runErr error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := w.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := db.New(tx)
	locked, err := q.LockRun(ctx, runID)
	if err != nil {
		return err
	}
	var status db.LutraTaskActionStatus
	var errorText string
	var nextAttempt pgtype.Timestamptz
	if runErr == nil {
		status = db.LutraTaskActionStatusSucceeded
	} else if attempt < 3 {
		delay := time.Second * time.Duration(1<<(attempt-1))
		status = db.LutraTaskActionStatusQueued
		errorText = runErr.Error()
		nextAttempt = pgtype.Timestamptz{Time: time.Now().Add(delay), Valid: true}
	} else {
		status = db.LutraTaskActionStatusFailed
		errorText = runErr.Error()
	}
	rows, err := q.FinishTaskAction(ctx, db.FinishTaskActionParams{
		ActionID: actionID, ClaimToken: token, Attempt: attempt,
		Status: status, OutputCbor: output, Error: errorText, NextAttemptAt: nextAttempt,
	})
	if err != nil {
		return err
	}
	if rows == 0 {
		slog.Debug("action completion discarded", "action_id", actionID, "attempt", attempt)
	}
	if rows == 1 {
		if err := w.Logs.AppendActionStatus(ctx, tx, actionID); err != nil {
			return err
		}
	}
	if rows == 1 && locked.RootActionID != nil && *locked.RootActionID == actionID {
		if err := w.Logs.AppendStatus(ctx, tx, runID); err != nil {
			return err
		}
	}
	rootTerminal := rows == 1 && status != db.LutraTaskActionStatusQueued && locked.RootActionID != nil && *locked.RootActionID == actionID
	if rootTerminal {
		if _, closeErr := q.CloseRunDescendants(ctx, db.CloseRunDescendantsParams{RunID: runID, ID: actionID}); closeErr != nil {
			return closeErr
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if rootTerminal {
		w.cancelDescendants(runID, actionID)
	}
	return nil
}

func (w *Worker) waitAction(ctx context.Context, identity taskContext, child uuid.UUID) error {
	actual, err := w.queries.GetTaskActionCaller(ctx, child)
	if err != nil || actual.RunID != identity.runID || actual.CallerActionID == nil || *actual.CallerActionID != identity.actionID {
		return invalidTask("task action is not a child of this task")
	}
	w.mu.Lock()
	active := w.active[identity.actionID]
	w.mu.Unlock()
	if active == nil || active.token != identity.token {
		return invalidTask("parent run is not active")
	}
	active.mu.Lock()
	if !active.slotHeld {
		active.mu.Unlock()
		return invalidTask("parent is already waiting")
	}
	active.slotHeld = false
	w.slots <- struct{}{}
	active.mu.Unlock()
	defer func() {
		select {
		case <-active.ctx.Done():
		case <-w.slots:
			active.mu.Lock()
			if active.ctx.Err() != nil {
				w.slots <- struct{}{}
				active.mu.Unlock()
				return
			}
			active.slotHeld = true
			active.mu.Unlock()
			restoreCtx, cancel := context.WithTimeout(active.ctx, 5*time.Second)
			defer cancel()
			if _, err := w.setWaiting(restoreCtx, active, false); err != nil && restoreCtx.Err() == nil {
				slog.Error("restore parent action", "action_id", active.actionID, "error", err)
			}
		}
	}()
	rows, err := w.setWaiting(ctx, active, true)
	if err != nil {
		return err
	}
	if rows != 1 {
		return invalidTask("parent run is not active")
	}
	for {
		status, err := w.queries.GetActionStatus(ctx, child)
		if err != nil {
			return err
		}
		if terminal(string(status)) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (w *Worker) setWaiting(ctx context.Context, active *activeRun, waiting bool) (int64, error) {
	tx, err := w.DB.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := db.New(tx)
	var rows int64
	if waiting {
		rows, err = q.MarkTaskActionWaiting(ctx, db.MarkTaskActionWaitingParams{ActionID: active.actionID, ClaimToken: active.token})
	} else {
		rows, err = q.RestoreTaskActionRunning(ctx, db.RestoreTaskActionRunningParams{ActionID: active.actionID, ClaimToken: active.token})
	}
	if err != nil {
		return 0, err
	}
	if rows > 0 {
		if err := w.Logs.AppendActionStatus(ctx, tx, active.actionID); err != nil {
			return 0, err
		}
		root, err := q.GetRunRootAction(ctx, active.runID)
		if err != nil {
			return 0, err
		}
		if root != nil && *root == active.actionID {
			if err := w.Logs.AppendStatus(ctx, tx, active.runID); err != nil {
				return 0, err
			}
		}
	}
	return rows, tx.Commit(ctx)
}

// executor returns the provider for an image name. Unconfigured providers
// fail explicitly until their adapters exist.
func (w *Worker) executor(name string) (Executor, error) {
	if name == localTaskImage {
		return &LocalExecutor{StoreArtifact: w.storeArtifact, LoadArtifact: w.loadArtifact, OpenBundle: w.openBundle}, nil
	}
	return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("%s executor is not configured", name))
}

func (w *Worker) executeEnvironment(ctx context.Context, imageKey []byte, token uuid.UUID, task *EnvironmentExecution) ([]byte, error) {
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
		w.TaskAPIHandler.ServeHTTP(response, request.WithContext(context.WithValue(request.Context(), taskContextKey{}, taskContext{runID: runID, actionID: actionID, token: token})))
	})
	executor, err := w.executor(task.Provider)
	if err != nil {
		return nil, err
	}
	_, _ = fmt.Fprintf(stderrWriter, "image build name=%s version=%s\n", task.Environment.Name, task.Environment.Version)
	image, err := w.ensureImage(ctx, imageKey, executor, task)
	if err != nil {
		return nil, err
	}
	job, err := executor.Run(ctx, image, task)
	if err != nil {
		return nil, err
	}
	rows, err := w.queries.SetTaskActionJob(ctx, db.SetTaskActionJobParams{ActionID: actionID, ClaimToken: token, JobID: job.ID()})
	if err != nil || rows != 1 {
		_ = job.Kill(context.Background())
		_, _ = job.Wait(ctx)
		if err == nil {
			err = errors.New("task action claim lost")
		}
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
