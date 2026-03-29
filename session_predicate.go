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

// InsertPredicate inserts a new predicate into the runtime.
func (s *Session) InsertPredicate(ctx context.Context, predicate Predicate) (Predicate, error) {
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

	// Check if already exists by hash to satisfy "InsertPredicate" callers that might be duplicating
	var existing Predicate
	query := `
		SELECT id, ontology_version_id, type, metadata, valid_from, valid_to, created_at, updated_at 
		FROM predicates 
		WHERE ontology_version_id = $1 AND dedup_hash = $2
		LIMIT 1`
	err = s.db.sqlDB.QueryRowContext(ctx, query, s.version.ID, dedupHash).
		Scan(&existing.ID, &existing.OntologyVersionID, &existing.Type, &existing.Metadata, &existing.ValidFrom, &existing.ValidTo, &existing.CreatedAt, &existing.UpdatedAt)
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Predicate{}, fmt.Errorf("checking for existing predicate: %w", err)
	}

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

// ResolveOrInsertPredicate resolves an existing predicate or inserts a new one.
func (s *Session) ResolveOrInsertPredicate(ctx context.Context, predicate Predicate) (Predicate, error) {
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

	// Standardize time to UTC for hashing and storage
	if !predicate.ValidFrom.IsZero() {
		predicate.ValidFrom = predicate.ValidFrom.UTC()
	}
	if !predicate.ValidTo.IsZero() {
		predicate.ValidTo = predicate.ValidTo.UTC()
	}
	predicate.OntologyVersionID = s.version.ID

	// Try to find existing predicate with same ontology_version_id and dedup_hash
	var p Predicate
	dedupHash := computePredicateDedupHash(predicate)
	query := `
		SELECT id, ontology_version_id, type, metadata, valid_from, valid_to, created_at, updated_at 
		FROM predicates 
		WHERE ontology_version_id = $1 AND dedup_hash = $2
		LIMIT 1`

	err = s.db.sqlDB.QueryRowContext(ctx, query, s.version.ID, dedupHash).
		Scan(&p.ID, &p.OntologyVersionID, &p.Type, &p.Metadata, &p.ValidFrom, &p.ValidTo, &p.CreatedAt, &p.UpdatedAt)

	if err == nil {
		p.ValidFrom = p.ValidFrom.UTC()
		p.ValidTo = p.ValidTo.UTC()
		return p, nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return Predicate{}, fmt.Errorf("searching for existing predicate: %w", err)
	}

	// Not found, insert new one
	return s.InsertPredicate(ctx, predicate)
}
