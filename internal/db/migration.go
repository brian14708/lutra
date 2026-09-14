package db

import (
	"context"
	"database/sql"
	"os"

	dbmigrations "github.com/brian14708/lutra/db/migrations"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

func Run() error {
	dsn := os.Getenv("DATABASE_URL")
	db, err := sql.Open("pgx", dsn)
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

	if _, err := provider.Up(context.Background()); err != nil {
		return err
	}
	return nil
}
