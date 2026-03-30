package acropora

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// MatchTriple canonicalizes the subject and object entities, attempts to match an existing triple in the
// current session ontology version, and inserts a new triple if no match is found.
//
// This method provides identity-aware triple materialization. It first resolves any subject or object
// aliases to their canonical root entities and checks if a triple with the same subject, predicate,
// and object already exists in the current session's ontology version. If a match is found, the
// existing triple is returned. Otherwise, a new triple is created and inserted into the runtime
// after validating the relationship against the ontology.
//
// MatchTriple is the primary public entrypoint for ensuring a triple exists within a session's
// context without creating duplicate entries for the same semantic relationship.
func (s *Session) MatchTriple(ctx context.Context, triple Triple) (Triple, error) {
	if triple.SubjectEntityID == "" {
		return Triple{}, errors.New("subject entity ID cannot be zero")
	}
	if triple.PredicateID == "" {
		return Triple{}, errors.New("predicate ID cannot be zero")
	}
	if triple.ObjectEntityID == "" {
		return Triple{}, errors.New("object entity ID cannot be zero")
	}

	// Canonicalize subject and object before matching
	var err error
	triple.SubjectEntityID, err = s.GetAntiAliasedEntityID(ctx, triple.SubjectEntityID)
	if err != nil {
		return Triple{}, fmt.Errorf("canonicalizing subject: %w", err)
	}
	triple.ObjectEntityID, err = s.GetAntiAliasedEntityID(ctx, triple.ObjectEntityID)
	if err != nil {
		return Triple{}, fmt.Errorf("canonicalizing object: %w", err)
	}

	// 1. Try to find existing triple
	var t Triple
	query := `
		SELECT id, ontology_version_id, subject_entity_id, predicate_id, object_entity_id, created_at, updated_at 
		FROM triples 
		WHERE ontology_version_id = $1 AND subject_entity_id = $2 AND predicate_id = $3 AND object_entity_id = $4
		LIMIT 1`

	err = s.db.sqlDB.QueryRowContext(ctx, query, s.version.ID, triple.SubjectEntityID, triple.PredicateID, triple.ObjectEntityID).
		Scan(&t.ID, &t.OntologyVersionID, &t.SubjectEntityID, &t.PredicateID, &t.ObjectEntityID, &t.CreatedAt, &t.UpdatedAt)

	if err == nil {
		return t, nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return Triple{}, fmt.Errorf("searching for existing triple: %w", err)
	}

	// 2. Not found, insert new one
	return s.insertTriple(ctx, triple)
}

// insertTriple is an internal helper that performs the actual row insertion.
// It is used after matching fails in MatchTriple or by other internal code paths
// that truly require forced insertion.
func (s *Session) insertTriple(ctx context.Context, triple Triple) (Triple, error) {
	if triple.SubjectEntityID == "" {
		return Triple{}, errors.New("subject entity ID cannot be zero")
	}
	if triple.PredicateID == "" {
		return Triple{}, errors.New("predicate ID cannot be zero")
	}
	if triple.ObjectEntityID == "" {
		return Triple{}, errors.New("object entity ID cannot be zero")
	}

	// Canonicalize subject and object before validation and insert
	var err error
	triple.SubjectEntityID, err = s.GetAntiAliasedEntityID(ctx, triple.SubjectEntityID)
	if err != nil {
		return Triple{}, fmt.Errorf("canonicalizing subject: %w", err)
	}
	triple.ObjectEntityID, err = s.GetAntiAliasedEntityID(ctx, triple.ObjectEntityID)
	if err != nil {
		return Triple{}, fmt.Errorf("canonicalizing object: %w", err)
	}

	// 1. Validate referenced runtime rows exist and are in the same ontology version
	var subjectType, predicateType, objectType string
	err = s.db.sqlDB.QueryRowContext(ctx,
		"SELECT type FROM entities WHERE id = $1 AND ontology_version_id = $2",
		triple.SubjectEntityID, s.version.ID).Scan(&subjectType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Triple{}, fmt.Errorf("subject entity %s not found in session ontology version", triple.SubjectEntityID)
		}
		return Triple{}, fmt.Errorf("checking subject entity: %w", err)
	}

	err = s.db.sqlDB.QueryRowContext(ctx,
		"SELECT type FROM predicates WHERE id = $1 AND ontology_version_id = $2",
		triple.PredicateID, s.version.ID).Scan(&predicateType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Triple{}, fmt.Errorf("predicate %s not found in session ontology version", triple.PredicateID)
		}
		return Triple{}, fmt.Errorf("checking predicate: %w", err)
	}

	err = s.db.sqlDB.QueryRowContext(ctx,
		"SELECT type FROM entities WHERE id = $1 AND ontology_version_id = $2",
		triple.ObjectEntityID, s.version.ID).Scan(&objectType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Triple{}, fmt.Errorf("object entity %s not found in session ontology version", triple.ObjectEntityID)
		}
		return Triple{}, fmt.Errorf("checking object entity: %w", err)
	}

	// 2. Validate ontology rule: subject type + predicate type + object type exists in ontology_triples
	var exists bool
	err = s.db.sqlDB.QueryRowContext(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM ontology_triples ot
			JOIN ontology_entities ose ON ot.subject_entity_id = ose.id
			JOIN ontology_predicates op ON ot.predicate_id = op.id
			JOIN ontology_entities ooe ON ot.object_entity_id = ooe.id
			WHERE ot.ontology_version_id = $1
			AND ose.type = $2 AND op.type = $3 AND ooe.type = $4
		)`,
		s.version.ID, subjectType, predicateType, objectType).Scan(&exists)
	if err != nil {
		return Triple{}, fmt.Errorf("validating triple against ontology: %w", err)
	}
	if !exists {
		return Triple{}, fmt.Errorf("triple (%s, %s, %s) not allowed by ontology version %s", subjectType, predicateType, objectType, s.version.Slug)
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

// GetOutgoingTriples returns all triples where subject_entity_id matches (expanded by alias group),
// scoped to the session's ontology version, and normalized to the canonical root.
func (s *Session) GetOutgoingTriples(ctx context.Context, entityID string) ([]Triple, error) {
	group, canonicalID, err := s.GetAliasGroupEntityIDs(ctx, entityID)
	if err != nil {
		return nil, fmt.Errorf("resolving alias group: %w", err)
	}

	rows, err := s.db.sqlDB.QueryContext(ctx,
		"SELECT id, ontology_version_id, subject_entity_id, predicate_id, object_entity_id, created_at, updated_at FROM triples WHERE subject_entity_id = ANY($1) AND ontology_version_id = $2",
		pq.Array(group), s.version.ID)
	if err != nil {
		return nil, fmt.Errorf("querying outgoing triples: %w", err)
	}
	defer rows.Close()

	var triples []Triple
	seen := make(map[string]bool)

	for rows.Next() {
		var t Triple
		if err := rows.Scan(&t.ID, &t.OntologyVersionID, &t.SubjectEntityID, &t.PredicateID, &t.ObjectEntityID, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scanning triple: %w", err)
		}

		// Normalize subject
		t.SubjectEntityID = canonicalID

		// Normalize object
		objCanonical, err := s.GetAntiAliasedEntityID(ctx, t.ObjectEntityID)
		if err == nil {
			t.ObjectEntityID = objCanonical
		}

		// Deduplicate based on canonical (Subject, Predicate, Object)
		key := fmt.Sprintf("%s|%s|%s", t.SubjectEntityID, t.PredicateID, t.ObjectEntityID)
		if !seen[key] {
			triples = append(triples, t)
			seen[key] = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating triples: %w", err)
	}
	return triples, nil
}
