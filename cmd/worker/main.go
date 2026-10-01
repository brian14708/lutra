// Command worker owns run leases and executes task graphs.
package main

import (
	"context"
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
	"github.com/brian14708/lutra/internal/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	db, err := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	storeClient, config, err := s3.NewFromEnv()
	if err != nil {
		logger.Error("blob store configuration failed", "error", err)
		os.Exit(1)
	}
	store := &minio.Core{Client: storeClient}
	logs := runlog.Service{DB: db, Store: store, Bucket: config.Bucket}
	if err := logs.ConfigureFromEnv(); err != nil {
		logger.Error("log configuration failed", "error", err)
		os.Exit(1)
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
	signer, err := s3.New(config.SignerConfig())
	if err != nil {
		logger.Error("blob signer configuration failed", "error", err)
		os.Exit(1)
	}
	path, handler = lutrav1connect.NewBlobServiceHandler(blob.Service{DB: db, Store: store, Signer: signer, Bucket: config.Bucket})
	mux.Handle(path, handler)
	worker := &lutra.RunWorker{DB: db, Capacity: capacity, MaxRuns: maxRuns, Logs: logs, Store: store, Bucket: config.Bucket, TaskAPIHandler: mux}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	worker.Start(ctx)
	<-ctx.Done()
	worker.Wait()
}
