// Command server runs the Lutra ConnectRPC HTTP server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpchealth"
	"connectrpc.com/grpcreflect"
	"connectrpc.com/otelconnect"
	"github.com/brian14708/lutra/db/migrations"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/blob"
	"github.com/brian14708/lutra/internal/lutra"
	"github.com/brian14708/lutra/internal/runlog"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

func main() {
	embeddedWorker := flag.Bool("dev-worker", false, "run an embedded worker for local development")
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(logger, *embeddedWorker); err != nil {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, embeddedWorker bool) error {
	db, err := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		return fmt.Errorf("database connection failed: %w", err)
	}
	defer db.Close()
	if err := db.Ping(context.Background()); err != nil {
		return fmt.Errorf("database unavailable: %w", err)
	}
	migrationDB := stdlib.OpenDBFromPool(db)
	defer func() { _ = migrationDB.Close() }()
	if err := migrations.Run(context.Background(), migrationDB); err != nil {
		return fmt.Errorf("database migration failed: %w", err)
	}
	blobService, err := blob.NewFromEnv(db)
	if err != nil {
		return fmt.Errorf("blob store configuration failed: %w", err)
	}
	exists, err := blobService.Store.BucketExists(context.Background(), blobService.Bucket)
	if err != nil {
		return fmt.Errorf("blob bucket check failed: %w", err)
	}
	if !exists {
		return fmt.Errorf("blob bucket %q does not exist", blobService.Bucket)
	}
	addr := os.Getenv("LUTRA_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	otelInterceptor, err := otelconnect.NewInterceptor()
	if err != nil {
		return fmt.Errorf("failed to initialize OpenTelemetry: %w", err)
	}
	interceptors := connect.WithInterceptors(otelInterceptor)

	mux := http.NewServeMux()
	rpcMux := http.NewServeMux()
	cleanupCtx, cancelCleanup := context.WithCancel(context.Background())
	defer cancelCleanup()
	go blobService.RunCleanup(cleanupCtx, time.Hour)
	blobPath, blobHandler := lutrav1connect.NewBlobServiceHandler(
		blobService,
		interceptors,
	)
	rpcMux.Handle(blobPath, blobHandler)
	logs := runlog.Service{DB: db, Store: blobService.Store, Bucket: blobService.Bucket}
	if err := logs.ConfigureFromEnv(); err != nil {
		return fmt.Errorf("log configuration failed: %w", err)
	}
	servicePath, serviceHandler := lutrav1connect.NewLutraServiceHandler(
		lutra.Service{DB: db, Logs: logs},
		interceptors,
		connect.WithReadMaxBytes(64<<20),
	)
	rpcMux.Handle(servicePath, serviceHandler)
	settingsPath, settingsHandler := lutrav1connect.NewSettingsServiceHandler(
		lutra.Service{DB: db},
		interceptors,
	)
	rpcMux.Handle(settingsPath, settingsHandler)
	logPath, logHandler := lutrav1connect.NewLogServiceHandler(
		logs,
		interceptors,
	)
	rpcMux.Handle(logPath, logHandler)
	healthPath, healthHandler := grpchealth.NewHandler(
		grpchealth.NewStaticChecker(lutrav1connect.LutraServiceName, lutrav1connect.BlobServiceName, lutrav1connect.SettingsServiceName, lutrav1connect.LogServiceName),
		interceptors,
	)
	rpcMux.Handle(healthPath, healthHandler)
	reflector := grpcreflect.NewStaticReflector(lutrav1connect.LutraServiceName, lutrav1connect.BlobServiceName, lutrav1connect.SettingsServiceName, lutrav1connect.LogServiceName)
	reflectionPath, reflectionHandler := grpcreflect.NewHandlerV1(reflector, interceptors)
	rpcMux.Handle(reflectionPath, reflectionHandler)
	reflectionAlphaPath, reflectionAlphaHandler := grpcreflect.NewHandlerV1Alpha(reflector, interceptors)
	rpcMux.Handle(reflectionAlphaPath, reflectionAlphaHandler)
	mux.Handle("/api/", http.StripPrefix("/api", rpcMux))
	// Serve the SPA build when it is present.
	const uiDir = "ui/dist"
	if info, err := os.Stat(uiDir); err == nil && info.IsDir() {
		mux.Handle("/", newSPAHandler(os.DirFS(uiDir)))
	}

	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("starting server", "addr", addr)
		serverErr <- server.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if embeddedWorker {
		worker := &lutra.RunWorker{DB: db, Logs: logs, Blobs: blobService, TaskAPIHandler: rpcMux}
		worker.Start(ctx)
		defer func() { stop(); worker.Wait() }()
	}
	select {
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("server stopped: %w", err)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("server shutdown failed: %w", err)
		}
	}
	return nil
}
