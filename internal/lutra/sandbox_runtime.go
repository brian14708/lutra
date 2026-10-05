package lutra

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/runlog"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sys/unix"
)

// SandboxRuntime owns local execution capacity and container lifecycle.
type SandboxRuntime struct {
	DB             *pgxpool.Pool
	Logs           runlog.Service
	Blobs          lutrav1connect.BlobServiceClient
	TaskAPIHandler http.Handler
	runtime        func() (string, error)
	slots          *semaphore.Weighted
	active         metric.Int64UpDownCounter
}

// Hold this lock for the executor lifetime so recovery cannot remove another
// process's active containers on the same local runtime and database.
func (r *SandboxRuntime) lockExecutor() (*os.File, error) {
	runtime, err := r.runtime()
	if err != nil {
		return nil, err
	}
	config := r.DB.Config().ConnConfig
	identity := fmt.Sprintf("%s:%d/%s/%s/%s/%s", config.Host, config.Port, config.Database, runtime, os.Getenv("DOCKER_HOST"), os.Getenv("CONTAINER_HOST"))
	key := sha256.Sum256([]byte(identity))
	path := filepath.Join(os.TempDir(), fmt.Sprintf("lutra-executor-%d-%x.lock", os.Getuid(), key))
	return lockExecutorFile(path)
}

func lockExecutorFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("another Lutra executor may own this runtime and database: %w", err)
	}
	return file, nil
}

func newSandboxRuntime(db *pgxpool.Pool, logs runlog.Service, blobs lutrav1connect.BlobServiceClient, handler http.Handler) (*SandboxRuntime, error) {
	capacity := 4
	if value := os.Getenv("LUTRA_WORKER_CONCURRENCY"); value != "" {
		var err error
		capacity, err = strconv.Atoi(value)
		if err != nil || capacity < 1 {
			return nil, errors.New("LUTRA_WORKER_CONCURRENCY must be positive")
		}
	}
	active, err := otel.Meter("lutra.execution").Int64UpDownCounter("lutra.execution.active", metric.WithDescription("Active builds and sandbox execution permits"))
	if err != nil {
		return nil, err
	}
	return &SandboxRuntime{DB: db, Logs: logs, Blobs: blobs, TaskAPIHandler: handler, runtime: sync.OnceValues(containerRuntime), slots: semaphore.NewWeighted(int64(capacity)), active: active}, nil
}

func (r *SandboxRuntime) acquire(ctx context.Context, phase LogPhase) (func(), error) {
	if err := r.slots.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	attributes := metric.WithAttributes(attribute.String("phase", string(phase)))
	r.active.Add(ctx, 1, attributes)
	return func() {
		r.active.Add(context.Background(), -1, attributes)
		r.slots.Release(1)
	}, nil
}

// Startup removes abandoned containers only when their actions belong to this
// database. Other development stacks can share the local container runtime.
func (r *SandboxRuntime) Recover(ctx context.Context) error {
	runtime, err := r.runtime()
	if err != nil {
		return err
	}
	output, err := exec.CommandContext(ctx, runtime, "ps", "--all", "--no-trunc", "--filter", "label=lutra.action", "--format", `{{.ID}} {{.Label "lutra.action"}}`).Output()
	if err != nil {
		return fmt.Errorf("list previous sandboxes: %w", err)
	}
	for line := range strings.Lines(string(output)) {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return errors.New("invalid sandbox listing")
		}
		id, err := uuid.Parse(fields[1])
		if err != nil {
			continue
		}
		if _, err := db.New(r.DB).ReadTaskAction(ctx, id); errors.Is(err, pgx.ErrNoRows) {
			continue
		} else if err != nil {
			return err
		}
		if err := removeContainer(ctx, runtime, fields[0]); err != nil {
			return err
		}
	}
	return nil
}
