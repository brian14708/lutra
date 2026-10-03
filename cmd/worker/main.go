// Command worker owns run leases and executes task graphs.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/blob"
	"github.com/brian14708/lutra/internal/lutra"
	"github.com/brian14708/lutra/internal/runlog"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(); err != nil {
		logger.Error("worker failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	db, err := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		return fmt.Errorf("database connection failed: %w", err)
	}
	defer db.Close()
	blobService, err := blob.NewFromEnv(db)
	if err != nil {
		return fmt.Errorf("blob store configuration failed: %w", err)
	}
	logs := runlog.Service{DB: db, Store: blobService.Store, Bucket: blobService.Bucket}
	if err := logs.ConfigureFromEnv(); err != nil {
		return fmt.Errorf("log configuration failed: %w", err)
	}
	capacity, _ := strconv.Atoi(os.Getenv("LUTRA_WORKER_CONCURRENCY"))
	// The worker binary owns execution. The API server can run without this
	// process and only needs the database and log service.
	maxRuns, _ := strconv.Atoi(os.Getenv("LUTRA_WORKER_MAX_RUNS"))
	mux := http.NewServeMux()
	path, handler := lutrav1connect.NewLutraServiceHandler(lutra.Service{DB: db, Logs: logs})
	mux.Handle(path, handler)
	path, handler = lutrav1connect.NewLogServiceHandler(logs)
	mux.Handle(path, handler)
	path, handler = lutrav1connect.NewBlobServiceHandler(blobService)
	mux.Handle(path, handler)
	worker := &lutra.RunWorker{DB: db, Capacity: capacity, MaxRuns: maxRuns, Logs: logs, Blobs: blobService, TaskAPIHandler: mux}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	worker.Start(ctx)
	<-ctx.Done()
	worker.Wait()
	return nil
}
