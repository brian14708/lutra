package lutra

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/cache"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/runlog"
	"github.com/brian14708/lutra/internal/tasktree"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

type RunWorker struct {
	DB                            *pgxpool.Pool
	Capacity, MaxRuns             int
	ProcessTarget, ProcessCeiling int
	Logs                          runlog.Service
	Blobs                         lutrav1connect.BlobServiceClient
	TaskAPIHandler                http.Handler
	slots                         tasktree.Slots
	processes                     *tasktree.ProcessPool
	cache                         *cache.Service
	cancelMu                      sync.Mutex
	owners                        map[uuid.UUID]context.CancelFunc
	wg                            sync.WaitGroup
}

func (w *RunWorker) Start(ctx context.Context) {
	if w.Capacity < 1 {
		w.Capacity = 4
	}
	if w.MaxRuns < 1 {
		w.MaxRuns = 16
	}
	w.owners = make(map[uuid.UUID]context.CancelFunc)
	w.slots = tasktree.NewSlots(w.Capacity)
	if w.ProcessTarget < 1 {
		w.ProcessTarget, _ = strconv.Atoi(os.Getenv("LUTRA_WORKER_PROCESS_TARGET"))
	}
	if w.ProcessTarget < 1 {
		w.ProcessTarget = max(4*w.Capacity, w.MaxRuns)
	}
	if w.ProcessCeiling < 1 {
		w.ProcessCeiling, _ = strconv.Atoi(os.Getenv("LUTRA_WORKER_PROCESS_CEILING"))
	}
	if w.ProcessCeiling < 1 {
		w.ProcessCeiling = 4 * w.ProcessTarget
	}
	w.processes = tasktree.NewProcessPool(w.ProcessTarget, w.ProcessCeiling)
	w.cache = cache.New(w.DB)
	w.wg.Add(1)
	go func() { defer w.wg.Done(); w.loop(ctx) }()
	w.wg.Add(1)
	go func() { defer w.wg.Done(); w.listenCancels(ctx) }()
}

func (w *RunWorker) Wait() { w.wg.Wait() }

func (w *RunWorker) listenCancels(ctx context.Context) {
	for ctx.Err() == nil {
		conn, err := w.DB.Acquire(ctx)
		if err == nil {
			_, err = conn.Exec(ctx, "LISTEN lutra_run_cancel")
			for err == nil && ctx.Err() == nil {
				var notification *pgconn.Notification
				notification, err = conn.Conn().WaitForNotification(ctx)
				if err == nil {
					if id, parseErr := uuid.Parse(notification.Payload); parseErr == nil {
						w.cancelMu.Lock()
						cancel := w.owners[id]
						w.cancelMu.Unlock()
						if cancel != nil {
							cancel()
						}
					}
				}
			}
			conn.Release()
		}
		if ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}
}

func (w *RunWorker) loop(ctx context.Context) {
	owners := make(chan struct{}, w.MaxRuns)
	for {
		select {
		case owners <- struct{}{}:
		case <-ctx.Done():
			return
		}
		token := uuid.New()
		run, err := db.New(w.DB).ClaimRun(ctx, db.ClaimRunParams{ClaimToken: token, LeaseSeconds: 30})
		if err != nil {
			<-owners
			if !errors.Is(err, pgx.ErrNoRows) && ctx.Err() == nil {
				slog.Error("claim run", "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		w.wg.Add(1)
		go func() { defer w.wg.Done(); defer func() { <-owners }(); w.drive(ctx, run.ID, token) }()
	}
}

func taskNode(row db.LoadRunTasksRow, runtime string, config RunConfigSnapshot) (tasktree.Node, error) {
	var spec lutrav1.ActionSpec
	if err := proto.Unmarshal(row.ActionSpec, &spec); err != nil {
		return tasktree.Node{}, err
	}
	state := tasktree.Pending
	switch row.Status {
	case db.LutraTaskActionStatusSucceeded:
		state = tasktree.Done
	case db.LutraTaskActionStatusFailed:
		state = tasktree.Failed
	case db.LutraTaskActionStatusCanceled:
		state = tasktree.Canceled
	}
	key := []byte(nil)
	if spec.GetCache() {
		key = cacheKey(row, &spec, runtime, config)
		if len(key) == 0 {
			return tasktree.Node{}, errors.New("invalid cached task environment")
		}
	}
	return tasktree.Node{ID: row.ID, ParentID: row.CallerActionID, Attempt: row.Attempts, Failures: row.Failures, MaxAttempts: max(spec.MaxAttempts, 1), State: state, ImageKey: row.ImageKey, CacheKey: key, Output: row.OutputCbor, Error: row.Error, NextAttemptAt: row.NextAttemptAt.Time}, nil
}

func cacheKey(row db.LoadRunTasksRow, spec *lutrav1.ActionSpec, runtime string, config RunConfigSnapshot) []byte {
	var environment lutrav1.EnvironmentSpec
	if err := proto.Unmarshal(row.EnvironmentSpec, &environment); err != nil {
		return nil
	}
	if row.EntrypointID < 1 || row.EntrypointID > int64(len(environment.Entrypoints)) {
		return nil
	}
	entrypoint := environment.Entrypoints[row.EntrypointID-1]
	req := &EnvironmentExecution{Spec: &environment, EntrypointID: uint32(row.EntrypointID), Config: config}
	env, projection, err := executionConfig(req)
	if err != nil {
		return nil
	}
	declaration, err := proto.MarshalOptions{Deterministic: true}.Marshal(entrypoint)
	if err != nil {
		return nil
	}
	version := spec.GetTaskVersion()
	if version == "" {
		version = environment.GetSourceUri()
	}
	image := environment.GetImage()
	imageDeclaration, err := proto.MarshalOptions{Deterministic: true}.Marshal(image)
	if err != nil {
		return nil
	}
	effective := RunConfigSnapshot{}
	for _, d := range declarations(&environment) {
		effective[d.path] = config[d.path]
	}
	dependencies := make([]map[string]string, 0, len(environment.GetDependencies()))
	for _, dependency := range environment.GetDependencies() {
		dependencies = append(dependencies, map[string]string{
			"name": dependency.GetName(), "version": dependency.GetVersion(),
		})
	}
	server := map[string]any{
		"profile":       "lutra.task-cache.v2",
		"environment":   environment.GetName(),
		"python_paths":  environment.GetPythonPaths(),
		"workdir":       environment.GetWorkdir(),
		"entrypoint_id": row.EntrypointID,
		"command":       entrypoint.GetCommand().GetArgs(),
		"task_version":  version,
		"input_cbor":    spec.GetInputCbor(),
		"provider":      row.Provider,
		"runtime":       runtime,
		"image_key":     row.ImageKey,
		"image": map[string]any{
			"name": image.GetName(), "from_image": image.GetFromImage(),
			"resources": map[string]uint64{
				"cpu_millis":   uint64(image.GetResources().GetCpuMillis()),
				"memory_bytes": image.GetResources().GetMemoryBytes(),
			},
			"declaration": imageDeclaration, "build_context_uri": image.GetBuildContextUri(),
			"platform": image.GetPlatform(), "python_requires": image.GetPythonRequires(),
		},
		"dependencies":       dependencies,
		"config":             projection,
		"config_declaration": declaration,
		"effective_env":      env,
		"effective_config":   effective,
	}
	mode, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		return nil
	}
	serverBytes, err := mode.Marshal(server)
	if err != nil {
		return nil
	}
	h := sha256.New()
	h.Write(serverBytes)
	return h.Sum(nil)
}

func environmentExecution(row db.LoadRunTasksRow, runID uuid.UUID, attempt int32, config RunConfigSnapshot) (*EnvironmentExecution, error) {
	var spec lutrav1.EnvironmentSpec
	var action lutrav1.ActionSpec
	if err := proto.Unmarshal(row.EnvironmentSpec, &spec); err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(row.ActionSpec, &action); err != nil {
		return nil, err
	}
	if row.EntrypointID < 1 || row.EntrypointID > int64(len(spec.Entrypoints)) {
		return nil, errors.New("entrypoint is not registered")
	}
	task := &EnvironmentExecution{Environment: &lutrav1.EnvironmentIdentifier{NamespaceId: row.NamespaceID.String(), Name: row.EnvironmentName, Version: row.Version}, EntrypointID: uint32(row.EntrypointID), Provider: row.Provider, Spec: &spec, Input: action.InputCbor, RunID: runID.String(), ActionID: row.ID.String(), Attempt: attempt}
	task.Config = config
	task.Environments = append([]*lutrav1.EnvironmentIdentifier{task.Environment}, spec.Dependencies...)
	return task, nil
}

func (w *RunWorker) drive(parent context.Context, runID, token uuid.UUID) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	w.cancelMu.Lock()
	w.owners[runID] = cancel
	w.cancelMu.Unlock()
	defer func() {
		w.cancelMu.Lock()
		delete(w.owners, runID)
		w.cancelMu.Unlock()
	}()
	q := db.New(w.DB)
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_, _ = q.ReleaseRunClaim(cleanup, db.ReleaseRunClaimParams{RunID: runID, ClaimToken: token})
	}()
	go w.renew(ctx, runID, token, cancel)
	tx, err := w.DB.Begin(ctx)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	tq := db.New(tx)
	if _, err = tq.LockRun(ctx, runID); err != nil {
		return
	}
	if _, err = tq.ResetRunActions(ctx, db.ResetRunActionsParams{RunID: runID, ClaimToken: token}); err != nil {
		return
	}
	rows, err := tq.LoadRunTasks(ctx, runID)
	if err != nil {
		return
	}
	if err = tx.Commit(ctx); err != nil {
		return
	}
	runtime, err := containerRuntime()
	if err != nil {
		slog.Error("select container runtime", "error", err)
		return
	}
	config, err := loadRunConfig(ctx, q, runID)
	if err != nil {
		slog.Error("load run configuration", "error", err)
		return
	}
	d := &runDriver{worker: w, ctx: ctx, runtime: runtime, runID: runID, token: token, rows: make(map[uuid.UUID]db.LoadRunTasksRow), images: make(map[string]*imageWait)}
	d.config = config
	snapshot := make([]tasktree.Node, 0, len(rows))
	for _, row := range rows {
		node, nodeErr := taskNode(row, runtime, config)
		if nodeErr != nil {
			slog.Error("restore task tree", "error", nodeErr)
			return
		}
		snapshot = append(snapshot, node)
		d.rows[row.ID] = row
	}
	d.coordinator = tasktree.New(snapshot, tasktree.Options{RunID: runID, ClaimToken: token, SlotPool: w.slots, ReadyQueue: 4 * w.Capacity, ProcessPool: w.processes, Store: taskStore{pool: w.DB, logs: w.Logs}, Runner: d, Images: d, Cache: d})
	if err := d.coordinator.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Debug("run finished", "run_id", runID, "error", err)
	}
}

func (w *RunWorker) renew(ctx context.Context, runID, token uuid.UUID, cancel context.CancelFunc) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			renewCtx, stop := context.WithTimeout(ctx, 5*time.Second)
			rows, err := db.New(w.DB).RenewRunLease(renewCtx, db.RenewRunLeaseParams{RunID: runID, ClaimToken: token, LeaseSeconds: 30})
			stop()
			if err != nil || rows != 1 {
				cancel()
				return
			}
		}
	}
}

type imageWait struct {
	done  chan struct{}
	image *Image
	err   error
}
type runDriver struct {
	config       RunConfigSnapshot
	worker       *RunWorker
	ctx          context.Context
	runtime      string
	runID, token uuid.UUID
	coordinator  *tasktree.Coordinator
	mu           sync.Mutex
	rows         map[uuid.UUID]db.LoadRunTasksRow
	images       map[string]*imageWait
}

func (d *runDriver) Ensure(ctx context.Context, key []byte) error {
	d.mu.Lock()
	wait := d.images[string(key)]
	if wait == nil {
		wait = &imageWait{done: make(chan struct{})}
		d.images[string(key)] = wait
		var row db.LoadRunTasksRow
		for _, candidate := range d.rows {
			if string(candidate.ImageKey) == string(key) {
				row = candidate
				break
			}
		}
		d.worker.wg.Add(1)
		go func() {
			defer d.worker.wg.Done()
			defer close(wait.done)
			task, err := environmentExecution(row, d.runID, row.Attempts, d.config)
			if err != nil {
				wait.err = err
				return
			}
			provider, err := d.worker.executor(row.Provider)
			if err != nil {
				wait.err = err
				return
			}
			runtime, err := containerRuntime()
			if err != nil {
				wait.err = err
				return
			}
			for _, phase := range []string{"pull", "build"} {
				reader, writer := io.Pipe()
				image := task.Spec.GetImage().GetFromImage()
				if phase == "build" {
					image = fmt.Sprintf("lutra:sha-%x", key)
				}
				logDone := d.worker.collectTaskLogs(runlog.TaskLogEvent{
					ActionID: row.ID.String(), Attempt: max(row.Attempts, 1),
					Phase: phase, Runtime: runtime, Image: image,
				}, d.runID, reader)
				defer func() {
					_ = writer.Close()
					<-logDone
				}()
				if phase == "pull" {
					task.PullOutput = writer
				} else {
					task.Stderr = writer
				}
			}
			wait.image, wait.err = ensureImage(d.ctx, key, provider, task)
		}()
	}
	d.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wait.done:
		return wait.err
	}
}

func (d *runDriver) Run(ctx context.Context, attempt tasktree.Attempt) ([]byte, error) {
	d.mu.Lock()
	row, ok := d.rows[attempt.NodeID]
	image := d.images[string(row.ImageKey)]
	d.mu.Unlock()
	if !ok || image == nil {
		return nil, errors.New("action image unavailable")
	}
	task, err := environmentExecution(row, d.runID, attempt.Number, d.config)
	if err != nil {
		return nil, err
	}
	identity := taskContext{TaskIdentity: TaskIdentity{RunID: d.runID, ActionID: attempt.NodeID, ClaimToken: d.token, Attempt: attempt.Number}, coordinator: d.coordinator}
	identity.add = func(ctx context.Context, child uuid.UUID) error {
		if _, err := d.coordinator.Get(ctx, child); err == nil {
			return nil
		}
		rows, err := db.New(d.worker.DB).LoadRunTasks(ctx, d.runID)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if row.ID != child {
				continue
			}
			node, err := taskNode(row, d.runtime, d.config)
			if err != nil {
				return err
			}
			d.mu.Lock()
			d.rows[child] = row
			d.mu.Unlock()
			return d.coordinator.Add(ctx, attempt.NodeID, attempt.Number, node)
		}
		return fmt.Errorf("child action %s is missing", child)
	}
	return d.worker.executeEnvironment(ctx, image.image, identity, task)
}
