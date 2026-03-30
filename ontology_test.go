package acropora

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		"acropora_db_version",
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
		Entities: []EntityDefinition{
			{Type: "Person"},
			{Type: "Place"},
		},
		Predicates: []PredicateDefinition{
			{Type: "lives_in"},
		},
	}
	def.Triples = []TripleDefinition{
		{
			Subject:   def.Entities[0],
			Predicate: def.Predicates[0],
			Object:    def.Entities[1],
		},
	}

	// 1. Valid seed
	version, err := a.SeedOntology(ctx, db, def, SeedOptions{Slug: "my-custom-slug"})
	if err != nil {
		t.Fatalf("failed to seed ontology: %v", err)
	}

	if version.ID == "" {
		t.Error("expected non-empty version ID")
	}
	if version.Slug != "my-custom-slug" {
		t.Errorf("expected slug 'my-custom-slug', got '%s'", version.Slug)
	}
	if version.Hash == "" {
		t.Error("expected non-empty version hash")
	}
	if version.CreatedAt.IsZero() {
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
	unknownEntity := EntityDefinition{Type: "Unknown"}
	invalidDef.Triples = []TripleDefinition{
		{
			Subject:   unknownEntity,
			Predicate: def.Predicates[0],
			Object:    def.Entities[1],
		},
	}
	_, err = a.SeedOntology(ctx, db, invalidDef, SeedOptions{})
	if err == nil {
		t.Error("expected error for invalid triple referencing missing entity, got nil")
	}

	// 3. Invalid triple (missing predicate)
	invalidDef = Definition{
		Entities:   def.Entities,
		Predicates: def.Predicates,
	}
	unknownPredicate := PredicateDefinition{Type: "unknown_predicate"}
	invalidDef.Triples = []TripleDefinition{
		{
			Subject:   def.Entities[0],
			Predicate: unknownPredicate,
			Object:    def.Entities[1],
		},
	}
	_, err = a.SeedOntology(ctx, db, invalidDef, SeedOptions{})
	if err == nil {
		t.Error("expected error for invalid triple referencing missing predicate, got nil")
	}

	// 4. Invalid triple (missing entity in maps - simulating bypass of validation)
	// We can't easily bypass validation because SeedOntology calls it,
	// but we can test that the error returned is the one from our new checks
	// if we were to somehow have a bug in validation or if it was optional.
}

func TestSeedOntology_MapLookupSafety(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	a, err := New(ctx, db)
	require.NoError(t, err)

	// Construct a definition that MIGHT pass validation if it was shallow,
	// but we want to ensure SeedOntology catches it.
	// Actually, validateOntologyDefinition is quite thorough now.
	// To truly test the NEW error messages in SeedOntology, we'd need to bypass validateOntologyDefinition.
	// Since it's internal to SeedOntology, we can just verify it doesn't panic and returns an error.

	def := Definition{
		Entities:   []EntityDefinition{{Type: "Person"}},
		Predicates: []PredicateDefinition{{Type: "works_at"}},
		Triples: []TripleDefinition{
			{
				Subject:   EntityDefinition{Type: "Person"},
				Predicate: PredicateDefinition{Type: "works_at"},
				Object:    EntityDefinition{Type: "Unknown"},
			},
		},
	}

	_, err = a.SeedOntology(ctx, db, def, SeedOptions{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "validation failed") // Caught by validation first
}

func TestDeterministicHashing(t *testing.T) {
	def := Definition{
		Entities:   []EntityDefinition{{Type: "Person"}, {Type: "Place"}},
		Predicates: []PredicateDefinition{{Type: "lives_in"}},
		Triples: []TripleDefinition{
			{
				Subject:   EntityDefinition{Type: "Person"},
				Predicate: PredicateDefinition{Type: "lives_in"},
				Object:    EntityDefinition{Type: "Place"},
			},
		},
	}
	hash1, _ := computeOntologyHash(def)
	hash2, _ := computeOntologyHash(def)
	if hash1 != hash2 {
		t.Errorf("hashes are not deterministic")
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
		Entities: []EntityDefinition{{Type: "A"}},
	}
	v1, err := a.SeedOntology(ctx, db, def1, SeedOptions{})
	if err != nil {
		t.Fatalf("failed to seed v1: %v", err)
	}

	// Sleep briefly to ensure different timestamps if needed, although DB now() should be enough for sequence
	time.Sleep(100 * time.Millisecond)

	// Seed second version
	def2 := Definition{
		Entities: []EntityDefinition{{Type: "B"}},
	}
	v2, err := a.SeedOntology(ctx, db, def2, SeedOptions{})
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
	defaultVersion, err := a.GetOntologyVersion(ctx, nil)
	if err != nil {
		t.Fatalf("GetOntologyVersion (default) failed: %v", err)
	}

	if defaultVersion.ID != v2.ID {
		t.Errorf("expected default version to be %s (v2), got %s", v2.ID, defaultVersion.ID)
	}
}

func TestOntologySlugs(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	a, err := New(ctx, db)
	if err != nil {
		t.Fatalf("failed to initialize acropora: %v", err)
	}

	// Ensure a clean slate
	_, _ = a.RawDB().ExecContext(ctx, "TRUNCATE TABLE ontology_versions RESTART IDENTITY CASCADE")

	def := Definition{Entities: []EntityDefinition{{Type: "SlugTest"}}}

	// 1. Seed without slug generates one
	v1, err := a.SeedOntology(ctx, db, def, SeedOptions{})
	if err != nil {
		t.Fatalf("failed to seed without slug: %v", err)
	}
	if v1.Slug == "" {
		t.Error("expected generated slug, got empty")
	}

	// 2. Seed with explicit slug
	explicitSlug := "explicit-slug-123"
	v2, err := a.SeedOntology(ctx, db, Definition{Entities: []EntityDefinition{{Type: "Explicit"}}}, SeedOptions{Slug: explicitSlug})
	if err != nil {
		t.Fatalf("failed to seed with explicit slug: %v", err)
	}
	if v2.Slug != explicitSlug {
		t.Errorf("expected slug %s, got %s", explicitSlug, v2.Slug)
	}

	// 3. Duplicate explicit slug fails
	_, err = a.SeedOntology(ctx, db, Definition{Entities: []EntityDefinition{{Type: "Duplicate"}}}, SeedOptions{Slug: explicitSlug})
	if err == nil {
		t.Error("expected error for duplicate slug, got nil")
	}
	assert.Contains(t, err.Error(), "failed to insert ontology version: duplicate slug")

	// 4. Verify slug format (codename-xxxx where xxxx is 4 chars)
	vFormat, err := a.SeedOntology(ctx, db, Definition{Entities: []EntityDefinition{{Type: "FormatTest"}}}, SeedOptions{})
	require.NoError(t, err)
	// slug should look like "codename-xxxx"
	// we expect at least one hyphen and exactly 4 characters after the last hyphen
	// Note: codename might contain hyphens, so we look for the last one
	slug := vFormat.Slug
	lastHyphen := -1
	for i := range slug {
		if slug[i] == '-' {
			lastHyphen = i
		}
	}
	require.NotEqual(t, -1, lastHyphen, "slug should contain at least one hyphen")
	suffix := slug[lastHyphen+1:]
	assert.Equal(t, 4, len(suffix), "suffix should be exactly 4 characters")

	// 5. Verify suffix is base64 (approximate check for base64 characters)
	for _, c := range suffix {
		isBase64 := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_'
		assert.True(t, isBase64, "suffix character %c should be base64-url", c)
	}

	// 6. Get by slug
	vFetched, err := a.GetOntologyVersion(ctx, GetOntologyVersionOptions{OptionSlug: explicitSlug})
	if err != nil {
		t.Fatalf("failed to get version by slug: %v", err)
	}
	if vFetched.ID != v2.ID {
		t.Errorf("expected fetched ID %s, got %s", v2.ID, vFetched.ID)
	}

	// 5. Get by hash
	vByHash, err := a.GetOntologyVersion(ctx, GetOntologyVersionOptions{OptionHash: v2.Hash})
	if err != nil {
		t.Fatalf("failed to get version by hash: %v", err)
	}
	if vByHash.ID != v2.ID {
		t.Errorf("expected fetched ID %s, got %s", v2.ID, vByHash.ID)
	}

	// 6. Get by ID
	vByID, err := a.GetOntologyVersion(ctx, GetOntologyVersionOptions{OptionID: v2.ID})
	if err != nil {
		t.Fatalf("failed to get version by ID: %v", err)
	}
	if vByID.ID != v2.ID {
		t.Errorf("expected fetched ID %s, got %s", v2.ID, vByID.ID)
	}

	// 7. Get by slug not found
	_, err = a.GetOntologyVersion(ctx, GetOntologyVersionOptions{OptionSlug: "non-existent-slug"})
	if err == nil {
		t.Error("expected error for non-existent slug, got nil")
	}

	// 8. Multiple filters error
	_, err = a.GetOntologyVersion(ctx, GetOntologyVersionOptions{
		OptionID:   v2.ID,
		OptionSlug: v2.Slug,
	})
	if err == nil {
		t.Error("expected error for multiple filters, got nil")
	}
}
