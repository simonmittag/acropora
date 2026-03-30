package acropora

import (
	"context"
	"database/sql"
	"fmt"

	acropora_db "github.com/simonmittag/acropora/internal/db"
)

// Options allows configuring the Acropora DB wrapper.
type Options struct {
	TablePrefix string
}

// DB is a wrapper type for our sql.DB handle.
type DB struct {
	sqlDB       *sql.DB
	tablePrefix string
}

// New creates a new Acropora DB wrapper, verifies the connection, and runs migrations.
func New(ctx context.Context, dbConn *sql.DB, opts Options) (*DB, error) {
	info(ctx, "initializing database")
	if err := dbConn.PingContext(ctx); err != nil {
		errorf(ctx, "failed to ping database: %v", err)
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	if opts.TablePrefix == "" {
		opts.TablePrefix = "acropora"
	}

	a := &DB{
		sqlDB:       dbConn,
		tablePrefix: opts.TablePrefix,
	}

	if err := a.migrate(ctx); err != nil {
		return nil, fmt.Errorf("running migrations: %w", err)
	}

	return a, nil
}

// BeginTx exposes transaction creation on the underlying DB handle.
func (d *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return d.sqlDB.BeginTx(ctx, opts)
}

// RawDB returns the underlying *sql.DB handle.
func (d *DB) RawDB() *sql.DB {
	return d.sqlDB
}

func (d *DB) migrate(ctx context.Context) error {
	info(ctx, "running migrations")

	if err := acropora_db.Migrate(ctx, d.sqlDB, d.tablePrefix); err != nil {
		return err
	}

	return nil
}
