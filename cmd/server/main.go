// Command server runs the Lutra ConnectRPC HTTP server.
package main

import (
	"context"
	"errors"
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
	"connectrpc.com/validate"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/lutra"
	"github.com/brian14708/lutra/internal/lutra/web"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
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
	_, handler := lutrav1connect.NewLutraServiceHandler(
		lutra.Service{},
		connect.WithInterceptors(otelInterceptor, validate.NewInterceptor()),
	)
	mux.Handle("/rpc/", http.StripPrefix("/rpc", handler))
	healthPath, healthHandler := grpchealth.NewHandler(
		grpchealth.NewStaticChecker(lutrav1connect.LutraServiceName),
		interceptors,
	)
	mux.Handle(healthPath, healthHandler)
	reflector := grpcreflect.NewStaticReflector(lutrav1connect.LutraServiceName)
	reflectionPath, reflectionHandler := grpcreflect.NewHandlerV1(reflector, interceptors)
	mux.Handle(reflectionPath, reflectionHandler)
	reflectionAlphaPath, reflectionAlphaHandler := grpcreflect.NewHandlerV1Alpha(reflector, interceptors)
	mux.Handle(reflectionAlphaPath, reflectionAlphaHandler)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	// Serve the SPA build when LUTRA_CONSOLE_DIR points at it.
	if consoleDir := os.Getenv("LUTRA_CONSOLE_DIR"); consoleDir != "" {
		if info, err := os.Stat(consoleDir); err == nil && info.IsDir() {
			mux.Handle("/", web.New(os.DirFS(consoleDir)))
		}
	}

	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
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
