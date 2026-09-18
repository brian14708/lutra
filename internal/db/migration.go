// Package db provides the PostgreSQL connection pool and schema migrations.
package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"

	dbmigrations "github.com/brian14708/lutra/db/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

const migrationLockName = "github.com/brian14708/lutra:migrations"

// OpenPool opens a shared PostgreSQL pool after applying migrations.
func OpenPool(ctx context.Context) (*pgxpool.Pool, error) {
	dsn := os.Getenv("DATABASE_URL")
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	// Goose and River maintain separate migration histories. They must still
	// run under one lock because both initialize objects in the same schema.
	if err := withMigrationLock(ctx, dsn, func(ctx context.Context, db *sql.DB) error {
		if err := runGoose(ctx, db); err != nil {
			return err
		}
		return migrateRiver(ctx, pool)
	}); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func runGoose(ctx context.Context, db *sql.DB) error {
	provider, err := goose.NewProvider(
		goose.DialectPostgres,
		db,
		dbmigrations.FS,
		goose.WithTableName("lutra_migrations"),
	)
	if err != nil {
		return err
	}

	if _, err := provider.Up(ctx); err != nil {
		return err
	}
	return nil
}

func migrateRiver(ctx context.Context, pool *pgxpool.Pool) error {
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), &rivermigrate.Config{Schema: "lutra"})
	if err != nil {
		return err
	}
	_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, &rivermigrate.MigrateOpts{})
	return err
}

// withMigrationLock keeps a dedicated SQL connection pinned while all work in
// fn runs. PostgreSQL releases a session advisory lock when that connection
// closes, but explicitly unlocking lets database/sql safely reuse the session.
func withMigrationLock(ctx context.Context, dsn string, fn func(context.Context, *sql.DB) error) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open migration lock connection: %w", err)
	}

	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock(hashtext($1))", migrationLockName); err != nil {
		_ = conn.Close()
		return fmt.Errorf("acquire migration lock: %w", err)
	}

	workErr := fn(ctx, db)
	unlockErr := unlockMigration(ctx, conn)
	closeErr := conn.Close()
	return errors.Join(workErr, unlockErr, closeErr)
}

func unlockMigration(ctx context.Context, conn *sql.Conn) error {
	unlockCtx := context.WithoutCancel(ctx)
	var unlocked bool
	if err := conn.QueryRowContext(
		unlockCtx,
		"SELECT pg_advisory_unlock(hashtext($1))",
		migrationLockName,
	).Scan(&unlocked); err != nil {
		return fmt.Errorf("release migration lock: %w", err)
	}
	if !unlocked {
		return errors.New("release migration lock: lock was not held")
	}
	return nil
}
