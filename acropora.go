package acropora

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Session is a version-bound runtime session.
type Session struct {
	db      *DB
	version OntologyVersion
}

// NewSession creates a new session bound to exactly one ontology version.
func (d *DB) NewSession(version OntologyVersion) *Session {
	return &Session{
		db:      d,
		version: version,
	}
}

// InsertEntity inserts a new entity into the runtime.
func (s *Session) InsertEntity(ctx context.Context, entity Entity) (Entity, error) {
	if entity.Name == "" {
		return Entity{}, errors.New("entity name cannot be empty")
	}

	// Validate against ontology
	var exists bool
	err := s.db.sqlDB.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM ontology_entities WHERE ontology_version_id = $1 AND name = $2)",
		s.version.ID, entity.Name).Scan(&exists)
	if err != nil {
		return Entity{}, fmt.Errorf("validating entity against ontology: %w", err)
	}
	if !exists {
		return Entity{}, fmt.Errorf("entity name %q not allowed by ontology version %s", entity.Name, s.version.Slug)
	}

	if entity.ID == "" {
		entity.ID = uuid.New().String()
	}
	entity.OntologyVersionID = s.version.ID
	if entity.Metadata == nil {
		entity.Metadata = json.RawMessage("{}")
	}

	now := time.Now()
	err = s.db.sqlDB.QueryRowContext(ctx,
		`INSERT INTO entities (id, ontology_version_id, name, metadata, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING created_at, updated_at`,
		entity.ID, entity.OntologyVersionID, entity.Name, entity.Metadata, now, now).Scan(&entity.CreatedAt, &entity.UpdatedAt)
	if err != nil {
		return Entity{}, fmt.Errorf("inserting entity: %w", err)
	}

	return entity, nil
}

// GetEntityByID fetches an entity by its ID, scoped to the session's ontology version.
func (s *Session) GetEntityByID(ctx context.Context, id string) (Entity, error) {
	var e Entity
	err := s.db.sqlDB.QueryRowContext(ctx,
		"SELECT id, ontology_version_id, name, metadata, created_at, updated_at FROM entities WHERE id = $1 AND ontology_version_id = $2",
		id, s.version.ID).Scan(&e.ID, &e.OntologyVersionID, &e.Name, &e.Metadata, &e.CreatedAt, &e.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Entity{}, fmt.Errorf("entity %s not found in session ontology version", id)
		}
		return Entity{}, fmt.Errorf("fetching entity: %w", err)
	}
	return e, nil
}

// InsertPredicate inserts a new predicate into the runtime.
func (s *Session) InsertPredicate(ctx context.Context, predicate Predicate) (Predicate, error) {
	if predicate.Name == "" {
		return Predicate{}, errors.New("predicate name cannot be empty")
	}

	if !predicate.ValidFrom.IsZero() && !predicate.ValidTo.IsZero() {
		if predicate.ValidTo.Before(predicate.ValidFrom) {
			return Predicate{}, errors.New("predicate ValidTo cannot be before ValidFrom")
		}
	}

	// Validate against ontology
	var exists bool
	err := s.db.sqlDB.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM ontology_predicates WHERE ontology_version_id = $1 AND name = $2)",
		s.version.ID, predicate.Name).Scan(&exists)
	if err != nil {
		return Predicate{}, fmt.Errorf("validating predicate against ontology: %w", err)
	}
	if !exists {
		return Predicate{}, fmt.Errorf("predicate name %q not allowed by ontology version %s", predicate.Name, s.version.Slug)
	}

	if predicate.ID == "" {
		predicate.ID = uuid.New().String()
	}
	predicate.OntologyVersionID = s.version.ID
	now := time.Now()
	err = s.db.sqlDB.QueryRowContext(ctx,
		`INSERT INTO predicates (id, ontology_version_id, name, valid_from, valid_to, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING created_at, updated_at`,
		predicate.ID, predicate.OntologyVersionID, predicate.Name, predicate.ValidFrom, predicate.ValidTo, now, now).Scan(&predicate.CreatedAt, &predicate.UpdatedAt)
	if err != nil {
		return Predicate{}, fmt.Errorf("inserting predicate: %w", err)
	}

	return predicate, nil
}

// GetPredicateByID fetches a predicate by its ID, scoped to the session's ontology version.
func (s *Session) GetPredicateByID(ctx context.Context, id string) (Predicate, error) {
	var p Predicate
	err := s.db.sqlDB.QueryRowContext(ctx,
		"SELECT id, ontology_version_id, name, valid_from, valid_to, created_at, updated_at FROM predicates WHERE id = $1 AND ontology_version_id = $2",
		id, s.version.ID).Scan(&p.ID, &p.OntologyVersionID, &p.Name, &p.ValidFrom, &p.ValidTo, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Predicate{}, fmt.Errorf("predicate %s not found in session ontology version", id)
		}
		return Predicate{}, fmt.Errorf("fetching predicate: %w", err)
	}
	return p, nil
}

// InsertTriple inserts a new triple into the runtime.
func (s *Session) InsertTriple(ctx context.Context, triple Triple) (Triple, error) {
	if triple.SubjectEntityID == "" {
		return Triple{}, errors.New("subject entity ID cannot be zero")
	}
	if triple.PredicateID == "" {
		return Triple{}, errors.New("predicate ID cannot be zero")
	}
	if triple.ObjectEntityID == "" {
		return Triple{}, errors.New("object entity ID cannot be zero")
	}

	// 1. Validate referenced runtime rows exist and are in the same ontology version
	var subjectName, predicateName, objectName string
	err := s.db.sqlDB.QueryRowContext(ctx,
		"SELECT name FROM entities WHERE id = $1 AND ontology_version_id = $2",
		triple.SubjectEntityID, s.version.ID).Scan(&subjectName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Triple{}, fmt.Errorf("subject entity %s not found in session ontology version", triple.SubjectEntityID)
		}
		return Triple{}, fmt.Errorf("checking subject entity: %w", err)
	}

	err = s.db.sqlDB.QueryRowContext(ctx,
		"SELECT name FROM predicates WHERE id = $1 AND ontology_version_id = $2",
		triple.PredicateID, s.version.ID).Scan(&predicateName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Triple{}, fmt.Errorf("predicate %s not found in session ontology version", triple.PredicateID)
		}
		return Triple{}, fmt.Errorf("checking predicate: %w", err)
	}

	err = s.db.sqlDB.QueryRowContext(ctx,
		"SELECT name FROM entities WHERE id = $1 AND ontology_version_id = $2",
		triple.ObjectEntityID, s.version.ID).Scan(&objectName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Triple{}, fmt.Errorf("object entity %s not found in session ontology version", triple.ObjectEntityID)
		}
		return Triple{}, fmt.Errorf("checking object entity: %w", err)
	}

	// 2. Validate ontology rule: subject name + predicate name + object name exists in ontology_triples
	var exists bool
	err = s.db.sqlDB.QueryRowContext(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM ontology_triples ot
			JOIN ontology_entities ose ON ot.subject_entity_id = ose.id
			JOIN ontology_predicates op ON ot.predicate_id = op.id
			JOIN ontology_entities ooe ON ot.object_entity_id = ooe.id
			WHERE ot.ontology_version_id = $1
			AND ose.name = $2 AND op.name = $3 AND ooe.name = $4
		)`,
		s.version.ID, subjectName, predicateName, objectName).Scan(&exists)
	if err != nil {
		return Triple{}, fmt.Errorf("validating triple against ontology: %w", err)
	}
	if !exists {
		return Triple{}, fmt.Errorf("triple (%s, %s, %s) not allowed by ontology version %s", subjectName, predicateName, objectName, s.version.Slug)
	}

	if triple.ID == "" {
		triple.ID = uuid.New().String()
	}
	triple.OntologyVersionID = s.version.ID
	now := time.Now()
	err = s.db.sqlDB.QueryRowContext(ctx,
		`INSERT INTO triples (id, ontology_version_id, subject_entity_id, predicate_id, object_entity_id, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING created_at, updated_at`,
		triple.ID, triple.OntologyVersionID, triple.SubjectEntityID, triple.PredicateID, triple.ObjectEntityID, now, now).Scan(&triple.CreatedAt, &triple.UpdatedAt)
	if err != nil {
		return Triple{}, fmt.Errorf("inserting triple: %w", err)
	}

	return triple, nil
}

// GetTripleByID fetches a triple by its ID, scoped to the session's ontology version.
func (s *Session) GetTripleByID(ctx context.Context, id string) (Triple, error) {
	var t Triple
	err := s.db.sqlDB.QueryRowContext(ctx,
		"SELECT id, ontology_version_id, subject_entity_id, predicate_id, object_entity_id, created_at, updated_at FROM triples WHERE id = $1 AND ontology_version_id = $2",
		id, s.version.ID).Scan(&t.ID, &t.OntologyVersionID, &t.SubjectEntityID, &t.PredicateID, &t.ObjectEntityID, &t.CreatedAt, &t.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Triple{}, fmt.Errorf("triple %s not found in session ontology version", id)
		}
		return Triple{}, fmt.Errorf("fetching triple: %w", err)
	}
	return t, nil
}

// GetOutgoingTriples returns all triples where subject_entity_id matches, scoped to the session's ontology version.
func (s *Session) GetOutgoingTriples(ctx context.Context, subjectEntityID string) ([]Triple, error) {
	rows, err := s.db.sqlDB.QueryContext(ctx,
		"SELECT id, ontology_version_id, subject_entity_id, predicate_id, object_entity_id, created_at, updated_at FROM triples WHERE subject_entity_id = $1 AND ontology_version_id = $2",
		subjectEntityID, s.version.ID)
	if err != nil {
		return nil, fmt.Errorf("querying outgoing triples: %w", err)
	}
	defer rows.Close()

	var triples []Triple
	for rows.Next() {
		var t Triple
		if err := rows.Scan(&t.ID, &t.OntologyVersionID, &t.SubjectEntityID, &t.PredicateID, &t.ObjectEntityID, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scanning triple: %w", err)
		}
		triples = append(triples, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating triples: %w", err)
	}

	return triples, nil
}
