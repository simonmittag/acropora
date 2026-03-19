package acropora

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const defaultDSN = "postgres://postgres:password@localhost:5432/acropora?sslmode=disable"

var testDSN string

func TestMain(m *testing.M) {
	testDSN = os.Getenv("DATABASE_URL")
	if testDSN == "" {
		testDSN = defaultDSN
	}
	os.Exit(m.Run())
}

func TestNew(t *testing.T) {
	db, err := sql.Open("pgx", testDSN)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	a, err := New(ctx, db)
	if err != nil {
		t.Fatalf("failed to initialize acropora: %v", err)
	}

	tables := []string{
		"ontology_versions",
		"ontology_entities",
		"ontology_predicates",
		"ontology_triples",
	}

	for _, table := range tables {
		var exists bool
		query := `SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_name = $1
		)`
		err := a.RawDB().QueryRowContext(ctx, query, table).Scan(&exists)
		if err != nil {
			t.Errorf("failed to check existence of table %s: %v", table, err)
		}
		if !exists {
			t.Errorf("table %s does not exist", table)
		}
	}
}
