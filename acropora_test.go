package acropora

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
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
		"entities",
		"predicates",
		"triples",
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

func TestEntityPersistence(t *testing.T) {
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

	tx, err := a.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	ovID := uuid.New().String()
	_, err = tx.ExecContext(ctx, "INSERT INTO ontology_versions (id, version_hash) VALUES ($1, $2)", ovID, "test-hash")
	if err != nil {
		t.Fatalf("failed to insert ontology version: %v", err)
	}

	entity := Entity{
		ID:                uuid.New().String(),
		OntologyVersionID: ovID,
		Name:              "test-entity",
		Metadata:          json.RawMessage(`{"key": "value"}`),
	}

	_, err = tx.ExecContext(ctx,
		"INSERT INTO entities (id, ontology_version_id, name, metadata) VALUES ($1, $2, $3, $4)",
		entity.ID, entity.OntologyVersionID, entity.Name, entity.Metadata)
	if err != nil {
		t.Fatalf("failed to insert entity: %v", err)
	}

	var persisted Entity
	err = tx.QueryRowContext(ctx,
		"SELECT id, ontology_version_id, name, metadata FROM entities WHERE id = $1",
		entity.ID).Scan(&persisted.ID, &persisted.OntologyVersionID, &persisted.Name, &persisted.Metadata)
	if err != nil {
		t.Fatalf("failed to query entity: %v", err)
	}

	if persisted.ID != entity.ID {
		t.Errorf("expected ID %s, got %s", entity.ID, persisted.ID)
	}
	if persisted.OntologyVersionID != entity.OntologyVersionID {
		t.Errorf("expected ontology version ID %s, got %s", entity.OntologyVersionID, persisted.OntologyVersionID)
	}
	if persisted.Name != entity.Name {
		t.Errorf("expected name %s, got %s", entity.Name, persisted.Name)
	}
	if string(persisted.Metadata) != string(entity.Metadata) {
		t.Errorf("expected metadata %s, got %s", string(entity.Metadata), string(persisted.Metadata))
	}
}

func TestPredicatePersistence(t *testing.T) {
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

	tx, err := a.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	ovID := uuid.New().String()
	_, err = tx.ExecContext(ctx, "INSERT INTO ontology_versions (id, version_hash) VALUES ($1, $2)", ovID, "test-hash-2")
	if err != nil {
		t.Fatalf("failed to insert ontology version: %v", err)
	}

	now := time.Now().Truncate(time.Microsecond) // Postgres precision
	from := now.Add(-time.Hour)
	to := now.Add(time.Hour)

	predicate := Predicate{
		ID:                uuid.New().String(),
		OntologyVersionID: ovID,
		Name:              "test-predicate",
		ValidFrom:         &from,
		ValidTo:           &to,
	}

	_, err = tx.ExecContext(ctx,
		"INSERT INTO predicates (id, ontology_version_id, name, valid_from, valid_to) VALUES ($1, $2, $3, $4, $5)",
		predicate.ID, predicate.OntologyVersionID, predicate.Name, predicate.ValidFrom, predicate.ValidTo)
	if err != nil {
		t.Fatalf("failed to insert predicate: %v", err)
	}

	var persisted Predicate
	err = tx.QueryRowContext(ctx,
		"SELECT id, ontology_version_id, name, valid_from, valid_to FROM predicates WHERE id = $1",
		predicate.ID).Scan(&persisted.ID, &persisted.OntologyVersionID, &persisted.Name, &persisted.ValidFrom, &persisted.ValidTo)
	if err != nil {
		t.Fatalf("failed to query predicate: %v", err)
	}

	if persisted.ID != predicate.ID {
		t.Errorf("expected ID %s, got %s", predicate.ID, persisted.ID)
	}
	if !persisted.ValidFrom.Equal(*predicate.ValidFrom) {
		t.Errorf("expected valid_from %v, got %v", predicate.ValidFrom, persisted.ValidFrom)
	}

	// Null handling test
	nullPredicate := Predicate{
		ID:                uuid.New().String(),
		OntologyVersionID: ovID,
		Name:              "null-predicate",
	}
	_, err = tx.ExecContext(ctx,
		"INSERT INTO predicates (id, ontology_version_id, name, valid_from, valid_to) VALUES ($1, $2, $3, $4, $5)",
		nullPredicate.ID, nullPredicate.OntologyVersionID, nullPredicate.Name, nullPredicate.ValidFrom, nullPredicate.ValidTo)
	if err != nil {
		t.Fatalf("failed to insert null predicate: %v", err)
	}

	var persistedNull Predicate
	err = tx.QueryRowContext(ctx,
		"SELECT id, ontology_version_id, name, valid_from, valid_to FROM predicates WHERE id = $1",
		nullPredicate.ID).Scan(&persistedNull.ID, &persistedNull.OntologyVersionID, &persistedNull.Name, &persistedNull.ValidFrom, &persistedNull.ValidTo)
	if err != nil {
		t.Fatalf("failed to query null predicate: %v", err)
	}

	if persistedNull.ValidFrom != nil {
		t.Errorf("expected nil valid_from, got %v", persistedNull.ValidFrom)
	}
	if persistedNull.ValidTo != nil {
		t.Errorf("expected nil valid_to, got %v", persistedNull.ValidTo)
	}
}

func TestTriplePersistence(t *testing.T) {
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

	tx, err := a.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Fixtures
	ovID := uuid.New().String()
	_, err = tx.ExecContext(ctx, "INSERT INTO ontology_versions (id, version_hash) VALUES ($1, $2)", ovID, "triple-test-hash")
	if err != nil {
		t.Fatalf("failed to insert ontology version: %v", err)
	}

	subjectID := uuid.New().String()
	_, err = tx.ExecContext(ctx, "INSERT INTO entities (id, ontology_version_id, name) VALUES ($1, $2, $3)", subjectID, ovID, "subject")
	if err != nil {
		t.Fatalf("failed to insert subject entity: %v", err)
	}

	objectID := uuid.New().String()
	_, err = tx.ExecContext(ctx, "INSERT INTO entities (id, ontology_version_id, name) VALUES ($1, $2, $3)", objectID, ovID, "object")
	if err != nil {
		t.Fatalf("failed to insert object entity: %v", err)
	}

	predicateID := uuid.New().String()
	_, err = tx.ExecContext(ctx, "INSERT INTO predicates (id, ontology_version_id, name) VALUES ($1, $2, $3)", predicateID, ovID, "predicate")
	if err != nil {
		t.Fatalf("failed to insert predicate: %v", err)
	}

	triple := Triple{
		ID:                uuid.New().String(),
		OntologyVersionID: ovID,
		SubjectEntityID:   subjectID,
		PredicateID:       predicateID,
		ObjectEntityID:    objectID,
	}

	// 1. Insert and read back
	_, err = tx.ExecContext(ctx,
		"INSERT INTO triples (id, ontology_version_id, subject_entity_id, predicate_id, object_entity_id) VALUES ($1, $2, $3, $4, $5)",
		triple.ID, triple.OntologyVersionID, triple.SubjectEntityID, triple.PredicateID, triple.ObjectEntityID)
	if err != nil {
		t.Fatalf("failed to insert triple: %v", err)
	}

	var persisted Triple
	err = tx.QueryRowContext(ctx,
		"SELECT id, ontology_version_id, subject_entity_id, predicate_id, object_entity_id FROM triples WHERE id = $1",
		triple.ID).Scan(&persisted.ID, &persisted.OntologyVersionID, &persisted.SubjectEntityID, &persisted.PredicateID, &persisted.ObjectEntityID)
	if err != nil {
		t.Fatalf("failed to query triple: %v", err)
	}

	if persisted.ID != triple.ID {
		t.Errorf("expected ID %s, got %s", triple.ID, persisted.ID)
	}

	// 2. Query outgoing triples for a subject
	var count int
	err = tx.QueryRowContext(ctx, "SELECT count(*) FROM triples WHERE subject_entity_id = $1", subjectID).Scan(&count)
	if err != nil {
		t.Fatalf("failed to query outgoing triples: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 outgoing triple, got %d", count)
	}

	// 3. Query incoming triples for an object
	err = tx.QueryRowContext(ctx, "SELECT count(*) FROM triples WHERE object_entity_id = $1", objectID).Scan(&count)
	if err != nil {
		t.Fatalf("failed to query incoming triples: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 incoming triple, got %d", count)
	}

	// 4. Duplicate exact triple insert fails
	_, err = tx.ExecContext(ctx,
		"INSERT INTO triples (id, ontology_version_id, subject_entity_id, predicate_id, object_entity_id) VALUES ($1, $2, $3, $4, $5)",
		uuid.New().String(), triple.OntologyVersionID, triple.SubjectEntityID, triple.PredicateID, triple.ObjectEntityID)
	if err == nil {
		t.Error("expected error when inserting duplicate triple, got nil")
	}
}
