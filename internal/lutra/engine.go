package lutra

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/grpchealth"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/runlog"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Engine owns server startup; execution policy lives in its adapter and runtime.
type Engine struct {
	Adapter      *DurableAdapter
	callback     string
	wg           sync.WaitGroup
	ready        atomic.Bool
	executorLock *os.File
}

func NewEngine(db *pgxpool.Pool, logs runlog.Service, blobs lutrav1connect.BlobServiceClient, handler http.Handler) (*Engine, error) {
	tasks, err := newSandboxRuntime(db, logs, blobs, handler)
	if err != nil {
		return nil, err
	}
	dispatch := &Dispatcher{Ingress: os.Getenv("LUTRA_RESTATE_INGRESS"), Admin: os.Getenv("LUTRA_RESTATE_ADMIN")}
	callback := os.Getenv("LUTRA_RESTATE_CALLBACK")
	if dispatch.Ingress == "" || dispatch.Admin == "" || callback == "" {
		return nil, errors.New("LUTRA_RESTATE_INGRESS, LUTRA_RESTATE_ADMIN and LUTRA_RESTATE_CALLBACK are required")
	}
	adapter := &DurableAdapter{DB: db, Logs: logs, Tasks: tasks, Dispatch: dispatch}
	return &Engine{Adapter: adapter, callback: callback}, nil
}

func (e *Engine) Handler() (http.Handler, error) { return e.Adapter.Handler() }

func (e *Engine) Check(ctx context.Context, request *grpchealth.CheckRequest) (*grpchealth.CheckResponse, error) {
	checker := grpchealth.NewStaticChecker(lutrav1connect.LutraServiceName, lutrav1connect.BlobServiceName, lutrav1connect.SettingsServiceName, lutrav1connect.LogServiceName)
	response, err := checker.Check(ctx, request)
	if err == nil && (request.Service == "" || request.Service == lutrav1connect.LutraServiceName) && !e.ready.Load() {
		response.Status = grpchealth.StatusNotServing
	}
	return response, err
}

func (e *Engine) Start(ctx context.Context) error {
	lock, err := e.Adapter.Tasks.lockExecutor()
	if err != nil {
		return err
	}
	if err := e.Adapter.Tasks.Recover(ctx); err != nil {
		_ = lock.Close()
		return err
	}
	e.executorLock = lock
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer e.ready.Store(false)
		for ctx.Err() == nil {
			requestCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			_, err := e.Adapter.Dispatch.request(requestCtx, http.MethodPost, e.Adapter.Dispatch.Admin+"/deployments", deploymentRequest{URI: e.callback, HTTP11: true}, "")
			cancel()
			if err == nil {
				e.ready.Store(true)
				e.deliverPending(ctx)
				return
			}
			slog.Warn("register execution backend", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}()
	return nil
}

func (e *Engine) deliverPending(ctx context.Context) {
	q := db.New(e.Adapter.DB)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		actions, err := q.PendingActionDispatches(ctx)
		if err != nil {
			slog.Warn("read dispatch outbox", "error", err)
		}
		for _, id := range actions {
			if _, err := e.Adapter.Dispatch.submit(ctx, id); err != nil {
				slog.Warn("deliver pending action", "action_id", id, "error", err)
				if err := q.DeferActionDispatch(ctx, id); err != nil && ctx.Err() == nil {
					slog.Warn("defer pending action", "action_id", id, "error", err)
				}
				continue
			}
			if err := q.CompleteActionDispatch(ctx, id); err != nil && ctx.Err() == nil {
				slog.Warn("acknowledge pending action", "action_id", id, "error", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (e *Engine) Wait() {
	e.wg.Wait()
	if e.executorLock != nil {
		_ = e.executorLock.Close()
	}
}

type deploymentRequest struct {
	URI    string `json:"uri"`
	HTTP11 bool   `json:"use_http_11"`
}
