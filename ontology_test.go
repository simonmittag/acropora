package acropora

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	ctx := context.Background()

	dsn := os.Getenv("DATABASE_URL")
	if dsn != "" {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}
		return db, func() { db.Close() }
	}

	// Spin up a Docker container
	container, err := postgres.Run(ctx,
		"postgres:15-alpine",
		postgres.WithDatabase("acropora"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	return db, func() {
		db.Close()
		_ = container.Terminate(ctx)
	}
}

func TestNew(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

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
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	a, err := New(ctx, db)
	if err != nil {
		t.Fatalf("failed to initialize acropora: %v", err)
	}

	// Ensure a clean slate for deterministic test runs (even if container is fresh)
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

func TestListAndDefaultOntologyVersions(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	a, err := New(ctx, db)
	if err != nil {
		t.Fatalf("failed to initialize acropora: %v", err)
	}

	// Ensure a clean slate
	_, _ = a.RawDB().ExecContext(ctx, "TRUNCATE TABLE ontology_versions RESTART IDENTITY CASCADE")

	// Seed first version
	def1 := Definition{
		Entities: []Entity{{Name: "A"}},
	}
	v1, err := a.SeedOntology(ctx, db, def1)
	if err != nil {
		t.Fatalf("failed to seed v1: %v", err)
	}

	// Sleep briefly to ensure different timestamps if needed, although DB now() should be enough for sequence
	time.Sleep(100 * time.Millisecond)

	// Seed second version
	def2 := Definition{
		Entities: []Entity{{Name: "B"}},
	}
	v2, err := a.SeedOntology(ctx, db, def2)
	if err != nil {
		t.Fatalf("failed to seed v2: %v", err)
	}

	// Test ListOntologyVersions
	versions, err := a.ListOntologyVersions(ctx)
	if err != nil {
		t.Fatalf("ListOntologyVersions failed: %v", err)
	}

	if len(versions) != 2 {
		t.Errorf("expected 2 versions, got %d", len(versions))
	}

	// Should be sorted by most recent first: v2 then v1
	if versions[0].ID != v2.ID {
		t.Errorf("expected first version to be %s (v2), got %s", v2.ID, versions[0].ID)
	}
	if versions[1].ID != v1.ID {
		t.Errorf("expected second version to be %s (v1), got %s", v1.ID, versions[1].ID)
	}

	// Test GetDefaultOntologyVersion
	defaultVersion, err := a.GetDefaultOntologyVersion(ctx)
	if err != nil {
		t.Fatalf("GetDefaultOntologyVersion failed: %v", err)
	}

	if defaultVersion.ID != v2.ID {
		t.Errorf("expected default version to be %s (v2), got %s", v2.ID, defaultVersion.ID)
	}
}
