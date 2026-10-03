// Command server runs the Lutra ConnectRPC HTTP server.
package main

import (
	"context"
	"errors"
	"flag"
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
	"github.com/brian14708/lutra/internal/lutra/web"
	"github.com/brian14708/lutra/internal/runlog"
	"github.com/brian14708/lutra/internal/s3"
	"github.com/brian14708/lutra/internal/settings"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/minio/minio-go/v7"
)

func main() {
	embeddedWorker := flag.Bool("dev-worker", false, "run an embedded worker for local development")
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	db, err := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := db.Ping(context.Background()); err != nil {
		logger.Error("database unavailable", "error", err)
		os.Exit(1)
	}
	if err := migrations.Run(context.Background(), stdlib.OpenDBFromPool(db)); err != nil {
		logger.Error("database migration failed", "error", err)
		os.Exit(1)
	}
	storeClient, storeConfig, err := s3.NewFromEnv()
	if err != nil {
		logger.Error("blob store configuration failed", "error", err)
		os.Exit(1)
	}
	exists, err := storeClient.BucketExists(context.Background(), storeConfig.Bucket)
	if err != nil {
		logger.Error("blob bucket check failed", "error", err)
		os.Exit(1)
	}
	if !exists {
		logger.Error("blob bucket does not exist", "bucket", storeConfig.Bucket)
		os.Exit(1)
	}
	signer, err := s3.New(storeConfig.SignerConfig())
	if err != nil {
		logger.Error("blob signer configuration failed", "error", err)
		os.Exit(1)
	}
	addr := os.Getenv("LUTRA_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	otelInterceptor, err := otelconnect.NewInterceptor()
	if err != nil {
		logger.Error("failed to initialize OpenTelemetry", "error", err)
		os.Exit(1)
	}
	interceptors := connect.WithInterceptors(otelInterceptor)

	mux := http.NewServeMux()
	rpcMux := http.NewServeMux()
	blobStore := &minio.Core{Client: storeClient}
	blobService := blob.Service{DB: db, Store: blobStore, Signer: signer, Bucket: storeConfig.Bucket}
	cleanupCtx, cancelCleanup := context.WithCancel(context.Background())
	defer cancelCleanup()
	go blobService.RunCleanup(cleanupCtx, time.Hour)
	blobPath, blobHandler := lutrav1connect.NewBlobServiceHandler(
		blobService,
		connect.WithInterceptors(otelInterceptor),
	)
	rpcMux.Handle(blobPath, blobHandler)
	logs := runlog.Service{DB: db, Store: blobStore, Bucket: storeConfig.Bucket}
	if err := logs.ConfigureFromEnv(); err != nil {
		logger.Error("log configuration failed", "error", err)
		os.Exit(1)
	}
	servicePath, serviceHandler := lutrav1connect.NewLutraServiceHandler(
		lutra.Service{DB: db, Logs: logs},
		connect.WithInterceptors(otelInterceptor),
		connect.WithReadMaxBytes(64<<20),
	)
	rpcMux.Handle(servicePath, serviceHandler)
	settingsPath, settingsHandler := lutrav1connect.NewSettingsServiceHandler(
		settings.Service{DB: db},
		connect.WithInterceptors(otelInterceptor),
	)
	rpcMux.Handle(settingsPath, settingsHandler)
	logPath, logHandler := lutrav1connect.NewLogServiceHandler(
		logs,
		connect.WithInterceptors(otelInterceptor),
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
		mux.Handle("/", web.New(os.DirFS(uiDir)))
	}

	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("starting server", "addr", addr)
		serverErr <- server.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *embeddedWorker {
		worker := &lutra.RunWorker{DB: db, Logs: logs, Blobs: blobService, TaskAPIHandler: rpcMux}
		worker.Start(ctx)
		defer func() { stop(); worker.Wait() }()
	}
	select {
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server stopped", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("server shutdown failed", "error", err)
			os.Exit(1)
		}
	}
}
