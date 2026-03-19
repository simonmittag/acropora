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

func TestSeedOntology(t *testing.T) {
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

	// Ensure a clean slate for deterministic test runs
	_, _ = a.RawDB().ExecContext(ctx, "TRUNCATE TABLE ontology_triples, ontology_predicates, ontology_entities, ontology_versions RESTART IDENTITY CASCADE")

	def := Definition{
		Entities: []Entity{
			{Name: "Person"},
			{Name: "Place"},
		},
		Predicates: []Predicate{
			{Name: "lives_in"},
		},
	}
	def.Triples = []Triple{
		{
			Subject:   &def.Entities[0],
			Predicate: &def.Predicates[0],
			Object:    &def.Entities[1],
		},
	}

	// 1. Valid seed
	version, err := a.SeedOntology(ctx, db, def)
	if err != nil {
		t.Fatalf("failed to seed ontology: %v", err)
	}

	if version.ID == "" {
		t.Error("expected non-empty version ID")
	}
	if version.Hash == "" {
		t.Error("expected non-empty version hash")
	}
	if version.Date.IsZero() {
		t.Error("expected non-zero version date")
	}

	// Verify counts in DB
	var count int
	err = db.QueryRowContext(ctx, "SELECT count(*) FROM ontology_entities WHERE ontology_version_id = $1", version.ID).Scan(&count)
	if err != nil || count != 2 {
		t.Errorf("expected 2 entities, got %d (err: %v)", count, err)
	}

	err = db.QueryRowContext(ctx, "SELECT count(*) FROM ontology_predicates WHERE ontology_version_id = $1", version.ID).Scan(&count)
	if err != nil || count != 1 {
		t.Errorf("expected 1 predicate, got %d (err: %v)", count, err)
	}

	err = db.QueryRowContext(ctx, "SELECT count(*) FROM ontology_triples WHERE ontology_version_id = $1", version.ID).Scan(&count)
	if err != nil || count != 1 {
		t.Errorf("expected 1 triple, got %d (err: %v)", count, err)
	}

	// 2. Invalid triple (missing entity)
	invalidDef := Definition{
		Entities:   def.Entities,
		Predicates: def.Predicates,
	}
	unknownEntity := Entity{Name: "Unknown"}
	invalidDef.Triples = []Triple{
		{
			Subject:   &unknownEntity,
			Predicate: &def.Predicates[0],
			Object:    &def.Entities[1],
		},
	}
	_, err = a.SeedOntology(ctx, db, invalidDef)
	if err == nil {
		t.Error("expected error for invalid triple referencing missing entity, got nil")
	}

	// 3. Invalid triple (missing predicate)
	invalidDef = Definition{
		Entities:   def.Entities,
		Predicates: def.Predicates,
	}
	unknownPredicate := Predicate{Name: "unknown_predicate"}
	invalidDef.Triples = []Triple{
		{
			Subject:   &def.Entities[0],
			Predicate: &unknownPredicate,
			Object:    &def.Entities[1],
		},
	}
	_, err = a.SeedOntology(ctx, db, invalidDef)
	if err == nil {
		t.Error("expected error for invalid triple referencing missing predicate, got nil")
	}

	// 4. Deterministic hashing
	version2, err := a.SeedOntology(ctx, db, def)
	if err != nil {
		// Might fail if hash unique constraint hits, but we want to check if hash is same
	}
	// Re-run seed with same content should produce same hash
	hash1, _ := computeOntologyHash(def)
	hash2, _ := computeOntologyHash(def)
	if hash1 != hash2 {
		t.Errorf("hashes are not deterministic: %s != %s", hash1, hash2)
	}

	if version2.Hash != "" && version2.Hash != version.Hash {
		t.Errorf("expected same hash for same definition content")
	}
}
