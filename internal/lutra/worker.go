package lutra

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"connectrpc.com/connect"
	taskv1 "github.com/brian14708/lutra/gen/lutra/task/v1"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/blob"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/taskstdio"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
)

type activeRun struct {
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

// Worker claims persistent runs and executes each attempt in a fresh bundle directory.
type Worker struct {
	DB             *pgxpool.Pool
	TaskAPIHandler http.Handler
	Store          *minio.Core
	Bucket         string
	Capacity       int
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
		id, token, attempt, err := w.claim(ctx)
		if err != nil || id == uuid.Nil {
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
		active := &activeRun{ctx: runCtx, cancel: cancel, token: token, slotHeld: true}
		w.mu.Lock()
		w.active[id] = active
		w.mu.Unlock()
		go w.execute(runCtx, id, attempt, active)
	}
}

func (w *Worker) claim(ctx context.Context) (uuid.UUID, uuid.UUID, int32, error) {
	token := uuid.New()
	claimed, err := w.queries.ClaimRun(ctx, db.ClaimRunParams{
		ClaimToken: token, LeaseSeconds: int32(leaseDuration / time.Second),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, uuid.Nil, 0, nil
	}
	return claimed.ID, token, claimed.Attempts, err
}

func (w *Worker) cancel(id uuid.UUID) {
	w.mu.Lock()
	active := w.active[id]
	w.mu.Unlock()
	if active != nil {
		active.cancel()
	}
}

func (w *Worker) execute(ctx context.Context, id uuid.UUID, attempt int32, active *activeRun) {
	stopRenewal := make(chan struct{})
	go w.renewLease(ctx, id, active, stopRenewal)
	defer func() {
		close(stopRenewal)
		active.cancel()
		active.mu.Lock()
		if active.slotHeld {
			w.slots <- struct{}{}
		}
		active.mu.Unlock()
		w.mu.Lock()
		if w.active[id] == active {
			delete(w.active, id)
		}
		w.mu.Unlock()
	}()
	claimed, err := w.queries.LoadClaimedRun(ctx, db.LoadClaimedRunParams{RunID: id, ClaimToken: active.token})
	if err != nil {
		if finishErr := w.finish(id, active.token, attempt, nil, err); finishErr != nil {
			slog.Error("finish run", "run_id", id, "error", finishErr)
		}
		return
	}
	task := &lutrav1.TaskSpec{
		Project: claimed.Project, Domain: claimed.Domain, Name: claimed.Name,
		Version: claimed.Version, Module: claimed.Module, Qualname: claimed.Qualname,
		Source: &lutrav1.SourceBundle{Uri: sourceURI(claimed.SourceSha256)},
		Image:  &lutrav1.TaskImage{Name: claimed.Image},
	}
	archive, err := w.loadSource(ctx, claimed.ObjectKey, claimed.SourceSha256)
	if err != nil {
		if finishErr := w.finish(id, active.token, attempt, nil, err); finishErr != nil {
			slog.Error("finish run", "run_id", id, "error", finishErr)
		}
		return
	}
	output, err := w.attempt(ctx, id, task, archive, claimed.InputCbor, attempt)
	if finishErr := w.finish(id, active.token, attempt, output, err); finishErr != nil {
		slog.Error("finish run", "run_id", id, "error", finishErr)
	}
}

func (w *Worker) loadSource(ctx context.Context, objectID uuid.UUID, digest []byte) ([]byte, error) {
	if w.Store == nil || w.Bucket == "" {
		return nil, errors.New("source blob store unavailable")
	}
	reader, _, _, err := w.Store.GetObject(ctx, w.Bucket, blob.ObjectKey(objectID), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	archive, err := io.ReadAll(io.LimitReader(reader, maxSourceSize+1))
	if err != nil {
		return nil, err
	}
	if len(archive) > maxSourceSize {
		return nil, errors.New("source bundle exceeds 64 MiB")
	}
	hash := sha256.Sum256(archive)
	if !bytes.Equal(hash[:], digest) {
		return nil, errors.New("source blob checksum mismatch")
	}
	return archive, nil
}

func (w *Worker) renewLease(ctx context.Context, id uuid.UUID, active *activeRun, stop <-chan struct{}) {
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
			rows, err := w.queries.RenewRunLease(renewCtx, db.RenewRunLeaseParams{
				LeaseSeconds: int32(leaseDuration / time.Second), RunID: id, ClaimToken: active.token,
			})
			cancel()
			if err != nil || rows != 1 {
				if err != nil && ctx.Err() == nil {
					slog.Error("renew run lease", "run_id", id, "error", err)
				}
				active.cancel()
				return
			}
		}
	}
}

func (w *Worker) finish(id, token uuid.UUID, attempt int32, output []byte, runErr error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var status string
	var errorText string
	var nextAttempt pgtype.Timestamptz
	if runErr == nil {
		status = "succeeded"
	} else if attempt < 3 {
		delay := time.Second * time.Duration(1<<(attempt-1))
		status = "queued"
		errorText = runErr.Error()
		nextAttempt = pgtype.Timestamptz{Time: time.Now().Add(delay), Valid: true}
	} else {
		status = "failed"
		errorText = runErr.Error()
	}
	rows, err := w.queries.FinishRun(ctx, db.FinishRunParams{
		RunID: id, ClaimToken: token, Attempt: attempt,
		Status: status, OutputCbor: output, Error: errorText, NextAttemptAt: nextAttempt,
	})
	if err != nil {
		return err
	}
	if rows == 0 {
		slog.Debug("run completion discarded", "run_id", id, "attempt", attempt)
	}
	return nil
}

func (w *Worker) waitChild(ctx context.Context, parent string, child uuid.UUID) error {
	parentID, err := uuid.Parse(parent)
	if err != nil {
		return invalidTask("invalid parent id")
	}
	actual, err := w.queries.GetChildParentID(ctx, child)
	if err != nil || actual == nil || *actual != parentID {
		return invalidTask("run is not a child of this task")
	}
	w.mu.Lock()
	active := w.active[parentID]
	w.mu.Unlock()
	if active == nil {
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
			if _, err := w.queries.RestoreRunRunning(restoreCtx, db.RestoreRunRunningParams{
				RunID: parentID, ClaimToken: active.token,
			}); err != nil && restoreCtx.Err() == nil {
				slog.Error("restore parent run", "run_id", parentID, "error", err)
			}
		}
	}()
	rows, err := w.queries.MarkRunWaiting(ctx, db.MarkRunWaitingParams{RunID: parentID, ClaimToken: active.token})
	if err != nil {
		return err
	}
	if rows != 1 {
		return invalidTask("parent run is not active")
	}
	for {
		status, err := w.queries.GetRunStatus(ctx, child)
		if err != nil {
			return err
		}
		if terminal(status) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (w *Worker) attempt(ctx context.Context, id uuid.UUID, task *lutrav1.TaskSpec, archive, input []byte, attempt int32) ([]byte, error) {
	if task.GetImage().GetName() != localTaskImage {
		return nil, fmt.Errorf("unsupported task image %q", task.GetImage().GetName())
	}
	dir, err := os.MkdirTemp("", "lutra-run-")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	path := filepath.Join(dir, "bundle.tar.zst")
	if err := os.WriteFile(path, archive, 0o600); err != nil {
		return nil, err
	}
	// Validate archive paths and size before running bundled code.
	if err := extractBundle(path, dir); err != nil {
		return nil, err
	}
	uvPath, err := exec.LookPath("uv")
	if err != nil {
		return nil, err
	}
	uvPath, err = filepath.EvalSymlinks(uvPath)
	if err != nil {
		return nil, err
	}
	hostPython, err := exec.LookPath("python3")
	if err != nil {
		return nil, err
	}
	hostPython, err = filepath.EvalSymlinks(hostPython)
	if err != nil {
		return nil, err
	}
	bwrapPath, err := exec.LookPath("bwrap")
	if err != nil {
		return nil, err
	}
	prep := exec.CommandContext(ctx, bwrapPath, append(sandboxArgs(dir), "--", uvPath, "sync", "--locked", "--no-dev", "--python", hostPython)...)
	prep.Env = []string{"PATH=" + os.Getenv("PATH")}
	prep.Stderr = os.Stderr
	if err := prep.Run(); err != nil {
		return nil, fmt.Errorf("uv sync: %w", err)
	}
	pythonPath := filepath.Join(dir, ".venv", "bin", "python")
	if _, err := os.Stat(pythonPath); err != nil {
		return nil, fmt.Errorf("task environment python is unavailable: %w", err)
	}
	args := append(sandboxArgs(dir),
		"--setenv", "LUTRA_TASK_PROJECT", task.Project,
		"--setenv", "LUTRA_TASK_DOMAIN", task.Domain,
		"--setenv", "LUTRA_TASK_NAME", task.Name,
		"--setenv", "LUTRA_TASK_MODULE", task.Module,
		"--setenv", "LUTRA_TASK_QUALNAME", task.Qualname,
		"--setenv", "LUTRA_TASK_VERSION", task.Version,
		"--setenv", "LUTRA_TASK_SOURCE_URI", task.GetSource().GetUri(),
		"--setenv", "LUTRA_TASK_IMAGE", task.GetImage().GetName(),
		"--setenv", "LUTRA_ATTEMPT", fmt.Sprint(attempt),
		"--setenv", "PYTHONPATH", dir+":"+filepath.Join(dir, "src")+":"+filepath.Join(dir, "sdk", "src"),
		"--", pythonPath, "-m", "lutra.serve",
	)
	command := exec.CommandContext(ctx, bwrapPath, args...)
	command.Env = prep.Env
	command.Stderr = os.Stderr
	process, err := taskstdio.Start(command)
	if err != nil {
		return nil, err
	}
	process.Transport.SetReverseHandler(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case lutrav1connect.LutraServiceCreateRunProcedure,
			lutrav1connect.LutraServiceGetRunProcedure,
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
		w.TaskAPIHandler.ServeHTTP(response, request.WithContext(context.WithValue(request.Context(), parentKey{}, id.String())))
	}))
	result, callErr := process.Client().Execute(ctx, connect.NewRequest(&taskv1.ExecuteRequest{InvocationId: id.String(), ContentType: "application/cbor", Input: input}))
	closeErr := process.Close()
	if callErr != nil {
		return nil, callErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if result.Msg.GetContentType() != "application/cbor" {
		return nil, fmt.Errorf("unsupported result content type %q", result.Msg.GetContentType())
	}
	return result.Msg.GetOutput(), nil
}

func sandboxArgs(dir string) []string {
	return []string{
		"--die-with-parent", "--new-session", "--unshare-pid", "--clearenv",
		"--ro-bind-try", "/nix/store", "/nix/store",
		"--ro-bind", "/usr", "/usr",
		"--ro-bind-try", "/etc/ssl", "/etc/ssl",
		"--ro-bind-try", "/etc/static/ssl", "/etc/static/ssl",
		"--ro-bind-try", "/etc/resolv.conf", "/etc/resolv.conf",
		"--ro-bind-try", "/etc/hosts", "/etc/hosts",
		"--ro-bind-try", "/etc/nsswitch.conf", "/etc/nsswitch.conf",
		"--ro-bind-try", "/etc/passwd", "/etc/passwd",
		"--ro-bind-try", "/etc/group", "/etc/group",
		"--ro-bind-try", "/bin", "/bin", "--ro-bind-try", "/lib", "/lib",
		"--ro-bind-try", "/lib64", "/lib64", "--tmpfs", "/tmp",
		"--bind", dir, dir, "--dev", "/dev", "--proc", "/proc",
		"--setenv", "PATH", os.Getenv("PATH"),
		"--setenv", "HOME", dir,
		"--setenv", "UV_CACHE_DIR", filepath.Join(dir, ".uv-cache"),
		"--chdir", dir,
	}
}
