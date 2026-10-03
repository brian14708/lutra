package migrations

import (
	"context"
	"database/sql"
	"embed"

	"github.com/pressly/goose/v3"
)

//go:embed *.sql
var migrations embed.FS

// Run applies all pending embedded migrations.
func Run(ctx context.Context, db *sql.DB) error {
	provider, err := goose.NewProvider(
		goose.DialectPostgres,
		db,
		migrations,
		goose.WithTableName("lutra_migrations"),
	)
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
}
