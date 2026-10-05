// Package lutra implements the Lutra service business logic.
package lutra

import (
	"errors"

	"connectrpc.com/connect"
	"github.com/brian14708/lutra/internal/runlog"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Service implements the core Lutra ConnectRPC API.
type Service struct {
	Durable *DurableAdapter
	DB      *pgxpool.Pool
	Logs    runlog.Service
}

func invalid(message string) error {
	return connect.NewError(connect.CodeInvalidArgument, errors.New(message))
}
