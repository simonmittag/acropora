package acropora

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// MatchPredicate canonicalizes the candidate predicate, attempts to match an existing canonical predicate in the
// current session ontology version, and inserts a new canonical predicate if no match is found.
//
// This method provides identity-aware predicate materialization. It first normalizes the input predicate's
// temporal window and metadata, and checks the session's ontology version for any existing predicate with the same
// identity hash. If a match is found, the existing canonical predicate is returned.
// Otherwise, a new predicate is created and inserted into the runtime after validating its type
// against the ontology.
//
// MatchPredicate is the primary public entrypoint for ensuring a predicate exists within a session's
// context without creating duplicate entries for the same real-world identity.
func (s *Session) MatchPredicate(ctx context.Context, predicate Predicate) (Predicate, error) {
	if predicate.Type == "" {
		return Predicate{}, errors.New("predicate type cannot be empty")
	}

	if !predicate.ValidFrom.IsZero() && !predicate.ValidTo.IsZero() {
		if predicate.ValidTo.Before(predicate.ValidFrom) {
			return Predicate{}, errors.New("predicate ValidTo cannot be before ValidFrom")
		}
	}

	// Standardize time to UTC for hashing and storage
	if !predicate.ValidFrom.IsZero() {
		predicate.ValidFrom = predicate.ValidFrom.UTC()
	}
	if !predicate.ValidTo.IsZero() {
		predicate.ValidTo = predicate.ValidTo.UTC()
	}
	predicate.OntologyVersionID = s.version.ID

	// 1. Try to find existing predicate with same ontology_version_id and dedup_hash
	dedupHash := computePredicateDedupHash(predicate)

	var p Predicate
	query := `
		SELECT id, ontology_version_id, type, metadata, valid_from, valid_to, created_at, updated_at 
		FROM predicates 
		WHERE ontology_version_id = $1 AND dedup_hash = $2
		LIMIT 1`

	err := s.db.sqlDB.QueryRowContext(ctx, query, s.version.ID, dedupHash).
		Scan(&p.ID, &p.OntologyVersionID, &p.Type, &p.Metadata, &p.ValidFrom, &p.ValidTo, &p.CreatedAt, &p.UpdatedAt)

	if err == nil {
		debug(ctx, "matched predicate %q (found)", predicate.Type)
		p.ValidFrom = p.ValidFrom.UTC()
		p.ValidTo = p.ValidTo.UTC()
		return p, nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return Predicate{}, fmt.Errorf("searching for existing predicate: %w", err)
	}

	// 2. Not found, insert new one
	debug(ctx, "matched predicate %q (inserted)", predicate.Type)
	return s.insertPredicate(ctx, predicate)
}

// insertPredicate is an internal helper that performs the actual row insertion.
// It is used after matching fails in MatchPredicate or by other internal code paths
// that truly require forced insertion.
func (s *Session) insertPredicate(ctx context.Context, predicate Predicate) (Predicate, error) {
	if predicate.Type == "" {
		return Predicate{}, errors.New("predicate type cannot be empty")
	}

	if !predicate.ValidFrom.IsZero() && !predicate.ValidTo.IsZero() {
		if predicate.ValidTo.Before(predicate.ValidFrom) {
			return Predicate{}, errors.New("predicate ValidTo cannot be before ValidFrom")
		}
	}

	// Validate against ontology
	var exists bool
	err := s.db.sqlDB.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM ontology_predicates WHERE ontology_version_id = $1 AND type = $2)",
		s.version.ID, predicate.Type).Scan(&exists)
	if err != nil {
		return Predicate{}, fmt.Errorf("validating predicate against ontology: %w", err)
	}
	if !exists {
		return Predicate{}, fmt.Errorf("predicate type %q not allowed by ontology version %s", predicate.Type, s.version.Slug)
	}

	if predicate.ID == "" {
		predicate.ID = uuid.New().String()
	}
	predicate.OntologyVersionID = s.version.ID
	if predicate.Metadata == nil {
		predicate.Metadata = json.RawMessage("{}")
	}
	if !predicate.ValidFrom.IsZero() {
		predicate.ValidFrom = predicate.ValidFrom.UTC()
	}
	if !predicate.ValidTo.IsZero() {
		predicate.ValidTo = predicate.ValidTo.UTC()
	}

	dedupHash := computePredicateDedupHash(predicate)

	now := time.Now().UTC()
	err = s.db.sqlDB.QueryRowContext(ctx,
		`INSERT INTO predicates (id, ontology_version_id, type, metadata, valid_from, valid_to, dedup_hash, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 RETURNING created_at, updated_at`,
		predicate.ID, predicate.OntologyVersionID, predicate.Type, predicate.Metadata, predicate.ValidFrom, predicate.ValidTo, dedupHash, now, now).Scan(&predicate.CreatedAt, &predicate.UpdatedAt)
	if err != nil {
		return Predicate{}, fmt.Errorf("inserting predicate: %w", err)
	}

	return predicate, nil
}

func computePredicateDedupHash(p Predicate) string {
	var fromStr, toStr string
	if !p.ValidFrom.IsZero() {
		fromStr = p.ValidFrom.UTC().Format(time.RFC3339)
	}
	if !p.ValidTo.IsZero() {
		toStr = p.ValidTo.UTC().Format(time.RFC3339)
	}

	meta := string(p.Metadata)
	if meta == "" {
		meta = "{}"
	}

	data := fmt.Sprintf("%s|%s|%s|%s|%s", p.OntologyVersionID, p.Type, fromStr, toStr, meta)
	h := sha256.Sum256([]byte(data))
	return hex.EncodeToString(h[:])
}

// GetPredicateByID fetches a predicate by its ID, scoped to the session's ontology version.
func (s *Session) GetPredicateByID(ctx context.Context, id string) (Predicate, error) {
	var p Predicate
	err := s.db.sqlDB.QueryRowContext(ctx,
		"SELECT id, ontology_version_id, type, metadata, valid_from, valid_to, created_at, updated_at FROM predicates WHERE id = $1 AND ontology_version_id = $2",
		id, s.version.ID).Scan(&p.ID, &p.OntologyVersionID, &p.Type, &p.Metadata, &p.ValidFrom, &p.ValidTo, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Predicate{}, fmt.Errorf("predicate %s not found in session ontology version", id)
		}
		return Predicate{}, fmt.Errorf("fetching predicate: %w", err)
	}
	return p, nil
}
