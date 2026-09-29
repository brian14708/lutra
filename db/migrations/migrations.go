package migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

// Run applies all pending migrations in FS.
func Run(ctx context.Context, db *sql.DB) error {
	provider, err := goose.NewProvider(
		goose.DialectPostgres,
		db,
		FS,
		goose.WithTableName("lutra_migrations"),
	)
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
}
