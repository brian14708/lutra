// Command server runs the Lutra ConnectRPC HTTP server.
package main

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpchealth"
	"connectrpc.com/grpcreflect"
	"connectrpc.com/otelconnect"
	"connectrpc.com/validate"
	dbmigrations "github.com/brian14708/lutra/db/migrations"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/lutra"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := runMigrations(); err != nil {
		logger.Error("database migration failed", "error", err)
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
	servicePath, serviceHandler := lutrav1connect.NewLutraServiceHandler(
		lutra.Service{},
		connect.WithInterceptors(otelInterceptor, validate.NewInterceptor()),
	)
	rpcMux.Handle(servicePath, serviceHandler)
	healthPath, healthHandler := grpchealth.NewHandler(
		grpchealth.NewStaticChecker(lutrav1connect.LutraServiceName),
		interceptors,
	)
	rpcMux.Handle(healthPath, healthHandler)
	reflector := grpcreflect.NewStaticReflector(lutrav1connect.LutraServiceName)
	reflectionPath, reflectionHandler := grpcreflect.NewHandlerV1(reflector, interceptors)
	rpcMux.Handle(reflectionPath, reflectionHandler)
	reflectionAlphaPath, reflectionAlphaHandler := grpcreflect.NewHandlerV1Alpha(reflector, interceptors)
	rpcMux.Handle(reflectionAlphaPath, reflectionAlphaHandler)
	mux.Handle("/api/", http.StripPrefix("/api", rpcMux))
	// Serve the SPA build when it is present.
	const consoleDir = "console/dist"
	if info, err := os.Stat(consoleDir); err == nil && info.IsDir() {
		mux.Handle("/", spaHandler(os.DirFS(consoleDir)))
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

func runMigrations() error {
	db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	provider, err := goose.NewProvider(
		goose.DialectPostgres,
		db,
		dbmigrations.FS,
		goose.WithTableName("lutra_migrations"),
	)
	if err != nil {
		return err
	}

	_, err = provider.Up(context.Background())
	return err
}

// spaHandler serves the Vite build and falls back to index.html for client routes.
func spaHandler(fsys fs.FS) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
		if name == "" {
			name = "index.html"
		}
		if serveSPAFile(w, r, fsys, name) {
			return
		}
		// Missing hashed assets must remain 404s instead of returning HTML.
		if strings.HasPrefix(name, "assets/") {
			http.NotFound(w, r)
			return
		}
		if name != "index.html" && serveSPAFile(w, r, fsys, "index.html") {
			return
		}
		http.NotFound(w, r)
	})
}

func serveSPAFile(w http.ResponseWriter, r *http.Request, fsys fs.FS, name string) bool {
	f, err := fsys.Open(name)
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil || info.IsDir() {
		return false
	}
	rs, ok := f.(io.ReadSeeker)
	if !ok {
		return false
	}
	if strings.HasPrefix(name, "assets/") {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else if name == "index.html" {
		w.Header().Set("Cache-Control", "no-cache")
	}
	http.ServeContent(w, r, info.Name(), info.ModTime(), rs)
	return true
}
