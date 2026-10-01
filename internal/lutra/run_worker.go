package lutra

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/graphexec"
	"github.com/brian14708/lutra/internal/runlog"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"google.golang.org/protobuf/proto"
)

type RunWorker struct {
	DB                *pgxpool.Pool
	Capacity, MaxRuns int
	Logs              runlog.Service
	Store             *minio.Core
	Bucket            string
	TaskAPIHandler    http.Handler
	slots             graphexec.Slots
	adapter           *Worker
	ctx               context.Context
	wg                sync.WaitGroup
}

func (w *RunWorker) Start(ctx context.Context) {
	if w.Capacity < 1 {
		w.Capacity = 4
	}
	if w.MaxRuns < 1 {
		w.MaxRuns = 16
	}
	w.ctx = ctx
	w.slots = graphexec.NewSlots(w.Capacity)
	w.adapter = &Worker{DB: w.DB, Logs: w.Logs, Store: w.Store, Bucket: w.Bucket, TaskAPIHandler: w.TaskAPIHandler, queries: db.New(w.DB)}
	w.wg.Add(2)
	go func() { defer w.wg.Done(); w.loop(ctx) }()
	go func() { defer w.wg.Done(); w.sweep(ctx) }()
}

func (w *RunWorker) Wait() { w.wg.Wait() }

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

func graphNode(row db.LoadRunGraphRow) (graphexec.Node, error) {
	var spec lutrav1.ActionSpec
	if err := proto.Unmarshal(row.ActionSpec, &spec); err != nil {
		return graphexec.Node{}, err
	}
	state := graphexec.Pending
	switch row.Status {
	case db.LutraTaskActionStatusSucceeded:
		state = graphexec.Done
	case db.LutraTaskActionStatusFailed:
		state = graphexec.Failed
	case db.LutraTaskActionStatusCanceled:
		state = graphexec.Canceled
	}
	key := []byte(nil)
	if spec.GetCache() {
		key = cacheKey(row, &spec)
		if len(key) == 0 {
			return graphexec.Node{}, errors.New("invalid cached task environment")
		}
	}
	return graphexec.Node{ID: row.ID, ParentID: row.CallerActionID, Attempt: row.Attempts, Failures: row.Failures, MaxAttempts: max(spec.MaxAttempts, 1), State: state, ImageKey: row.ImageKey, CacheKey: key, Output: row.OutputCbor, Error: row.Error, NextAttemptAt: row.NextAttemptAt.Time}, nil
}

func cacheKey(row db.LoadRunGraphRow, spec *lutrav1.ActionSpec) []byte {
	var environment lutrav1.EnvironmentSpec
	if err := proto.Unmarshal(row.EnvironmentSpec, &environment); err != nil {
		return nil
	}
	image := environment.GetImage()
	dependencies := make([]map[string]string, 0, len(environment.GetDependencies()))
	for _, dependency := range environment.GetDependencies() {
		dependencies = append(dependencies, map[string]string{
			"name": dependency.GetName(), "version": dependency.GetVersion(),
		})
	}
	server := map[string]any{
		"profile":         "lutra.task-cache-server.v1",
		"provider":        row.Provider,
		"runtime_version": os.Getenv("LUTRA_PYTHON_RUNTIME_VERSION"),
		"image": map[string]any{
			"name": image.GetName(), "reference": image.GetReference(),
			"resources": map[string]uint64{
				"cpu_millis":   uint64(image.GetResources().GetCpuMillis()),
				"memory_bytes": image.GetResources().GetMemoryBytes(),
			},
			"env": image.GetEnvVars(), "build_command": image.GetBuildCommand().GetArgs(),
			"workdir": image.GetWorkdir(),
		},
		"dependencies": dependencies,
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
	h.Write([]byte("lutra.task-cache.v1\x00"))
	h.Write(spec.GetCacheKey())
	h.Write(serverBytes)
	return h.Sum(nil)
}

func environmentExecution(row db.LoadRunGraphRow, runID uuid.UUID, attempt int32) (*EnvironmentExecution, error) {
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
	task.Environments = append([]*lutrav1.EnvironmentIdentifier{task.Environment}, spec.Dependencies...)
	return task, nil
}

func (w *RunWorker) drive(parent context.Context, runID, token uuid.UUID) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
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
	rows, err := tq.LoadRunGraph(ctx, runID)
	if err != nil {
		return
	}
	if err = tx.Commit(ctx); err != nil {
		return
	}
	d := &runDriver{worker: w, runID: runID, token: token, rows: make(map[uuid.UUID]db.LoadRunGraphRow), images: make(map[string]*imageWait)}
	snapshot := make([]graphexec.Node, 0, len(rows))
	for _, row := range rows {
		node, nodeErr := graphNode(row)
		if nodeErr != nil {
			slog.Error("restore graph", "error", nodeErr)
			return
		}
		snapshot = append(snapshot, node)
		d.rows[row.ID] = row
	}
	d.graph = graphexec.New(snapshot, graphexec.Options{RunID: runID, ClaimToken: token, SlotPool: w.slots, Store: graphStore{pool: w.DB, logs: w.Logs}, Runner: d, Images: d, Cache: d})
	if err := d.graph.Run(ctx); err != nil && ctx.Err() == nil {
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

func (w *RunWorker) sweep(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := db.New(w.DB).SweepExpiredImageBuilds(ctx); err != nil && ctx.Err() == nil {
				slog.Error("expire image builds", "error", err)
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
	worker       *RunWorker
	runID, token uuid.UUID
	graph        *graphexec.Executor
	mu           sync.Mutex
	rows         map[uuid.UUID]db.LoadRunGraphRow
	images       map[string]*imageWait
}

func (d *runDriver) Lookup(ctx context.Context, owner uuid.UUID, key []byte) ([]byte, error, bool) {
	return taskCache{worker: d.worker}.Lookup(ctx, owner, key)
}

func (d *runDriver) Store(ctx context.Context, owner uuid.UUID, key, output []byte, err error) error {
	return taskCache{worker: d.worker}.Store(ctx, owner, key, output, err)
}

func (d *runDriver) Ensure(ctx context.Context, key []byte) error {
	d.mu.Lock()
	wait := d.images[string(key)]
	if wait == nil {
		wait = &imageWait{done: make(chan struct{})}
		d.images[string(key)] = wait
		var row db.LoadRunGraphRow
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
			task, err := environmentExecution(row, d.runID, row.Attempts)
			if err != nil {
				wait.err = err
				return
			}
			provider, err := d.worker.adapter.executor(row.Provider)
			if err != nil {
				wait.err = err
				return
			}
			wait.image, wait.err = d.worker.adapter.ensureImage(d.worker.ctx, key, provider, task)
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

func (d *runDriver) Run(ctx context.Context, attempt graphexec.Attempt) ([]byte, error) {
	d.mu.Lock()
	row, ok := d.rows[attempt.NodeID]
	image := d.images[string(row.ImageKey)]
	d.mu.Unlock()
	if !ok || image == nil {
		return nil, errors.New("action image unavailable")
	}
	task, err := environmentExecution(row, d.runID, attempt.Number)
	if err != nil {
		return nil, err
	}
	identity := taskContext{runID: d.runID, actionID: attempt.NodeID, token: d.token, attempt: attempt.Number, graph: d.graph}
	identity.add = func(ctx context.Context, child uuid.UUID) error {
		if _, err := d.graph.Get(ctx, child); err == nil {
			return nil
		}
		rows, err := db.New(d.worker.DB).LoadRunGraph(ctx, d.runID)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if row.ID != child {
				continue
			}
			node, err := graphNode(row)
			if err != nil {
				return err
			}
			d.mu.Lock()
			d.rows[child] = row
			d.mu.Unlock()
			return d.graph.Add(ctx, attempt.NodeID, attempt.Number, node)
		}
		return fmt.Errorf("child action %s is missing", child)
	}
	return d.worker.adapter.executeEnvironment(ctx, image.image, identity, task)
}
