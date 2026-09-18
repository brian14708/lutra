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
	"strings"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpchealth"
	"connectrpc.com/grpcreflect"
	"connectrpc.com/otelconnect"
	"connectrpc.com/validate"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/auth"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/lutra"
	"github.com/brian14708/lutra/internal/lutra/web"
	"github.com/brian14708/lutra/internal/s3"
	"github.com/brian14708/lutra/internal/telemetry"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func main() {
	addrFlag := flag.String("addr", "", "HTTP server listen address")
	flag.Parse()
	logger := slog.New(telemetry.NewSlogHandler(slog.NewTextHandler(os.Stdout, nil)))
	if err := run(logger, *addrFlag); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, configuredAddr string) error {
	slog.SetDefault(logger)
	telemetryProviders, err := telemetry.Setup(context.Background(), "lutra-server")
	if err != nil {
		return fmt.Errorf("telemetry initialization failed: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := telemetryProviders.Shutdown(shutdownCtx); err != nil {
			logger.Error("telemetry shutdown failed", "error", err)
		}
	}()
	if os.Getenv("LUTRA_JWT_KEY") == "" {
		return errors.New("LUTRA_JWT_KEY must be configured for worker attempt tokens")
	}
	pool, err := db.OpenPool(context.Background())
	if err != nil {
		return fmt.Errorf("database initialization failed: %w", err)
	}
	defer pool.Close()
	authStore, err := auth.NewStore(context.Background(), pool)
	if err != nil {
		return fmt.Errorf("authentication initialization failed: %w", err)
	}
	if err := authStore.Bootstrap(context.Background()); err != nil {
		return fmt.Errorf("authentication bootstrap failed: %w", err)
	}
	addr := configuredAddr
	if addr == "" {
		addr = ":8080"
	}

	// Business RPCs additionally validate requests and authenticate callers.
	otelInterceptor, err := otelconnect.NewInterceptor(otelconnect.WithTrustRemote())
	if err != nil {
		return fmt.Errorf("telemetry RPC interceptor initialization failed: %w", err)
	}
	rpcInterceptors := connect.WithInterceptors(otelInterceptor, validate.NewInterceptor(), authStore.Interceptor())
	worker := lutra.NewWorkerEngine(authStore, 4)
	workerService := lutra.NewWorkerService(authStore, worker)
	worker.SetAttemptTokenIssuer(workerService.IssueAttemptToken)
	artifactObject, artifactConfig, artifactErr := s3.NewFromEnv()
	if artifactErr == nil {
		artifactErr = s3.EnsureBucket(context.Background(), artifactObject, artifactConfig)
	}
	if artifactErr != nil {
		logger.Warn("artifact object store is unavailable", "error", artifactErr)
	} else {
		worker.SetArtifactStore(artifactObject, artifactConfig.Bucket)
	}
	if err := worker.ConfigureRiver(pool, logger); err != nil {
		return fmt.Errorf("river initialization failed: %w", err)
	}
	if err := worker.Start(context.Background()); err != nil {
		return fmt.Errorf("river worker startup failed: %w", err)
	}

	mux := http.NewServeMux()
	rpcMux := http.NewServeMux()
	authPath, authHandler := lutrav1connect.NewAuthServiceHandler(auth.NewService(authStore), rpcInterceptors)
	rpcMux.Handle(authPath, authHandler)
	projectPath, projectHandler := lutrav1connect.NewProjectServiceHandler(lutra.NewProjectService(authStore), rpcInterceptors)
	rpcMux.Handle(projectPath, projectHandler)
	artifactPath, artifactHandler := lutrav1connect.NewArtifactServiceHandler(lutra.NewArtifactService(authStore, artifactObject, artifactConfig.Bucket), rpcInterceptors)
	rpcMux.Handle(artifactPath, artifactHandler)
	taskPath, taskHandler := lutrav1connect.NewTaskServiceHandler(lutra.NewTaskService(authStore, artifactObject, artifactConfig.Bucket), rpcInterceptors)
	rpcMux.Handle(taskPath, taskHandler)
	runPath, runHandler := lutrav1connect.NewRunServiceHandler(lutra.NewRunService(authStore, worker), rpcInterceptors)
	rpcMux.Handle(runPath, runHandler)
	healthPath, healthHandler := grpchealth.NewHandler(
		grpchealth.NewStaticChecker(lutrav1connect.AuthServiceName, lutrav1connect.ProjectServiceName, lutrav1connect.ArtifactServiceName, lutrav1connect.TaskServiceName, lutrav1connect.RunServiceName),
	)
	rpcMux.Handle(healthPath, healthHandler)
	reflector := grpcreflect.NewStaticReflector(lutrav1connect.AuthServiceName, lutrav1connect.ProjectServiceName, lutrav1connect.ArtifactServiceName, lutrav1connect.TaskServiceName, lutrav1connect.RunServiceName)
	reflectionPath, reflectionHandler := grpcreflect.NewHandlerV1(reflector)
	rpcMux.Handle(reflectionPath, reflectionHandler)
	reflectionAlphaPath, reflectionAlphaHandler := grpcreflect.NewHandlerV1Alpha(reflector)
	rpcMux.Handle(reflectionAlphaPath, reflectionAlphaHandler)
	const apiPrefix = "/api"
	mux.Handle(apiPrefix+"/", http.StripPrefix(apiPrefix, rpcMux))
	// WorkerService uses attempt-scoped JWTs and is kept out of the public
	// reflection and health surfaces. The route prefix keeps it distinct from
	// user-facing RPCs while allowing a single HTTP listener.
	_, workerHandler := lutrav1connect.NewWorkerServiceHandler(workerService, connect.WithInterceptors(otelInterceptor, validate.NewInterceptor()))
	const workerAPIPrefix = "/worker-api"
	mux.Handle(workerAPIPrefix+"/", http.StripPrefix(workerAPIPrefix, workerHandler))
	// Serve the SPA build when LUTRA_CONSOLE_DIR points at it.
	if consoleDir := os.Getenv("LUTRA_CONSOLE_DIR"); consoleDir != "" {
		if info, err := os.Stat(consoleDir); err == nil && info.IsDir() {
			mux.Handle("/", web.New(os.DirFS(consoleDir)))
		}
	}

	server := &http.Server{
		Addr: addr,
		Handler: otelhttp.NewHandler(
			mux,
			"lutra.http",
			otelhttp.WithFilter(httpTraceFilter),
			otelhttp.WithSpanNameFormatter(httpSpanName),
		),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("starting server", "addr", addr)
		serverErr <- server.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("server stopped: %w", err)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := worker.Stop(shutdownCtx); err != nil {
			logger.Error("worker shutdown failed", "error", err)
		}
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("server shutdown failed: %w", err)
		}
	}
	return nil
}

// httpSpanName gives non-RPC HTTP spans a useful name. If this formatter is
// used for an API request, the request path still contains the fully-qualified
// Connect procedure even though ServeMux only exposes the mounted prefix.
func httpSpanName(_ string, r *http.Request) string {
	for _, prefix := range []string{"/api/", "/worker-api/"} {
		if strings.HasPrefix(r.URL.Path, prefix) {
			return "connectrpc " + r.URL.Path
		}
	}
	return r.Method
}

// httpTraceFilter leaves RPC tracing to otelconnect. Wrapping the same request
// with otelhttp would create a second server span and hide the remote
// ConnectRPC parent from the RPC interceptor.
func httpTraceFilter(r *http.Request) bool {
	return !strings.HasPrefix(r.URL.Path, "/api/") && !strings.HasPrefix(r.URL.Path, "/worker-api/")
}
