package acropora

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrations embed.FS

// DB is a wrapper type for our sql.DB handle.
type DB struct {
	sqlDB *sql.DB
}

// New creates a new Acropora DB wrapper, verifies the connection, and runs migrations.
func New(ctx context.Context, db *sql.DB) (*DB, error) {
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	a := &DB{
		sqlDB: db,
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
	goose.SetBaseFS(migrations)

	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}

	if err := goose.UpContext(ctx, d.sqlDB, "migrations"); err != nil {
		return err
	}

	return nil
}
