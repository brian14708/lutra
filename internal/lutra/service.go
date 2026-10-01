// Package lutra implements the Lutra service business logic.
package lutra

import (
	"github.com/brian14708/lutra/internal/runlog"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Service implements the core Lutra ConnectRPC API.
type Service struct {
	DB   *pgxpool.Pool
	Logs runlog.Service
}
