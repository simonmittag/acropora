package acropora

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/lib/pq"
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

// normalizeCanonicalName helper for entity-name normalization.
func normalizeCanonicalName(name string) string {
	// 1. Remove non-printable / control characters
	var b strings.Builder
	for _, r := range name {
		if unicode.IsPrint(r) && r != 0 {
			b.WriteRune(r)
		}
	}
	s := b.String()

	// 2. Lowercase
	s = strings.ToLower(s)

	// 3. Trim leading/trailing whitespace
	s = strings.TrimSpace(s)

	// 4. Collapse repeated internal whitespace to single spaces
	fields := strings.Fields(s)
	return strings.Join(fields, " ")
}

// InsertEntity inserts a new entity into the runtime.
func (s *Session) InsertEntity(ctx context.Context, entity Entity) (Entity, error) {
	if entity.Type == "" {
		return Entity{}, errors.New("entity type cannot be empty")
	}

	trimmedRaw := strings.TrimSpace(entity.RawName)
	if trimmedRaw == "" {
		return Entity{}, errors.New("entity raw name cannot be empty")
	}

	// Compute CanonicalName internally
	entity.CanonicalName = normalizeCanonicalName(entity.RawName)

	// Validate against ontology
	var exists bool
	err := s.db.sqlDB.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM ontology_entities WHERE ontology_version_id = $1 AND type = $2)",
		s.version.ID, entity.Type).Scan(&exists)
	if err != nil {
		return Entity{}, fmt.Errorf("validating entity against ontology: %w", err)
	}
	if !exists {
		return Entity{}, fmt.Errorf("entity type %q not allowed by ontology version %s", entity.Type, s.version.Slug)
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
		`INSERT INTO entities (id, ontology_version_id, type, raw_name, canonical_name, metadata, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 RETURNING created_at, updated_at`,
		entity.ID, entity.OntologyVersionID, entity.Type, entity.RawName, entity.CanonicalName, entity.Metadata, now, now).Scan(&entity.CreatedAt, &entity.UpdatedAt)
	if err != nil {
		return Entity{}, fmt.Errorf("inserting entity: %w", err)
	}

	return entity, nil
}

// ResolveOrInsertEntity attempts to find an existing entity by normalizing the name and checking both
// canonical names and known aliases. If not found, it creates a new entity.
func (s *Session) ResolveOrInsertEntity(ctx context.Context, entity Entity) (Entity, error) {
	if entity.Type == "" {
		return Entity{}, errors.New("entity type cannot be empty")
	}

	trimmedRaw := strings.TrimSpace(entity.RawName)
	if trimmedRaw == "" {
		return Entity{}, errors.New("entity raw name cannot be empty")
	}

	// 1. Try to find existing entity by name (handles canonical name and aliases)
	found, err := s.GetEntityByRawName(ctx, entity.RawName)
	if err == nil {
		// Found existing entity. Ensure types match if requested.
		// If the existing entity has a different type, we could either return an error,
		// or just return the found entity. The requirements say "takes a name and an optional entity type".
		// In our Entity struct, Type is required for InsertEntity.
		return found, nil
	}

	// If the error is anything other than "not found", return it.
	if !strings.Contains(err.Error(), "not found") {
		return Entity{}, fmt.Errorf("resolving entity: %w", err)
	}

	// 2. Not found, so insert new entity
	return s.InsertEntity(ctx, entity)
}

// GetEntityByID fetches an entity by its ID, scoped to the session's ontology version.
func (s *Session) GetEntityByID(ctx context.Context, id string) (Entity, error) {
	var e Entity
	err := s.db.sqlDB.QueryRowContext(ctx,
		"SELECT id, ontology_version_id, type, raw_name, canonical_name, metadata, created_at, updated_at FROM entities WHERE id = $1 AND ontology_version_id = $2",
		id, s.version.ID).Scan(&e.ID, &e.OntologyVersionID, &e.Type, &e.RawName, &e.CanonicalName, &e.Metadata, &e.CreatedAt, &e.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Entity{}, fmt.Errorf("entity %s not found in session ontology version", id)
		}
		return Entity{}, fmt.Errorf("fetching entity: %w", err)
	}
	return e, nil
}

// GetEntityByRawName fetches an entity by its raw name, applying conservative canonicalization and anti-alias resolution.
func (s *Session) GetEntityByRawName(ctx context.Context, rawName string) (Entity, error) {
	if strings.TrimSpace(rawName) == "" {
		return Entity{}, errors.New("raw name cannot be empty")
	}

	canonicalName := normalizeCanonicalName(rawName)

	var e Entity
	err := s.db.sqlDB.QueryRowContext(ctx,
		"SELECT id, ontology_version_id, type, raw_name, canonical_name, metadata, created_at, updated_at FROM entities WHERE canonical_name = $1 AND ontology_version_id = $2",
		canonicalName, s.version.ID).Scan(&e.ID, &e.OntologyVersionID, &e.Type, &e.RawName, &e.CanonicalName, &e.Metadata, &e.CreatedAt, &e.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Entity{}, fmt.Errorf("entity %q not found", rawName)
		}
		return Entity{}, fmt.Errorf("fetching entity by raw name: %w", err)
	}

	// Internal anti-alias resolution
	canonicalID, err := s.GetCanonicalEntityID(ctx, e.ID)
	if err != nil {
		return Entity{}, fmt.Errorf("resolving entity alias: %w", err)
	}

	if canonicalID != e.ID {
		return s.GetEntityByID(ctx, canonicalID)
	}

	return e, nil
}

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

	// Canonicalize subject and object before validation and insert
	var err error
	triple.SubjectEntityID, err = s.GetCanonicalEntityID(ctx, triple.SubjectEntityID)
	if err != nil {
		return Triple{}, fmt.Errorf("canonicalizing subject: %w", err)
	}
	triple.ObjectEntityID, err = s.GetCanonicalEntityID(ctx, triple.ObjectEntityID)
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
		objCanonical, err := s.GetCanonicalEntityID(ctx, t.ObjectEntityID)
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

// LinkEntityAlias links an alias entity to a canonical root entity.
func (s *Session) LinkEntityAlias(ctx context.Context, aliasEntityID, canonicalEntityID string, metadata json.RawMessage) (EntityAlias, error) {
	if aliasEntityID == "" || canonicalEntityID == "" {
		return EntityAlias{}, errors.New("entity IDs cannot be empty")
	}
	if aliasEntityID == canonicalEntityID {
		return EntityAlias{}, errors.New("cannot link entity to itself")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return EntityAlias{}, err
	}
	defer tx.Rollback()

	// 1. Ensure both exist and belong to session ontology version
	var aType, cType string
	err = tx.QueryRowContext(ctx, "SELECT type FROM entities WHERE id = $1 AND ontology_version_id = $2", aliasEntityID, s.version.ID).Scan(&aType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return EntityAlias{}, fmt.Errorf("alias entity %s not found in session ontology version", aliasEntityID)
		}
		return EntityAlias{}, fmt.Errorf("checking alias entity %s: %w", aliasEntityID, err)
	}
	err = tx.QueryRowContext(ctx, "SELECT type FROM entities WHERE id = $1 AND ontology_version_id = $2", canonicalEntityID, s.version.ID).Scan(&cType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return EntityAlias{}, fmt.Errorf("canonical entity %s not found in session ontology version", canonicalEntityID)
		}
		return EntityAlias{}, fmt.Errorf("checking canonical entity %s: %w", canonicalEntityID, err)
	}

	// 2. Resolve canonicalEntityID to its canonical root
	rootID, err := s.getCanonicalEntityIDTx(ctx, tx, canonicalEntityID)
	if err != nil {
		return EntityAlias{}, err
	}

	// 3. Resolve aliasEntityID to its root (to check for cycles or redundant moves)
	aliasRootID, err := s.getCanonicalEntityIDTx(ctx, tx, aliasEntityID)
	if err != nil {
		return EntityAlias{}, err
	}

	if aliasRootID == rootID {
		return EntityAlias{}, errors.New("entities are already in the same alias group")
	}

	// 4. Reject if operation would create cycle (if rootID is an alias of aliasEntityID - impossible if graph is flat and we resolved rootID)
	// But let's be explicit: if someone tries to link A -> B when B -> A exists.
	// getCanonicalEntityIDTx(B) would return A. So we'd try to link A -> A, which is rejected above.

	// 5. Reparent any aliases currently pointing to aliasEntityID (or its group if it was a root)
	// Actually, if aliasEntityID was a root, we move it and ALL its children to the new root.
	// If aliasEntityID was already an alias, we move JUST it (it has no children).
	// But according to rule 4: "migrate C's alias link to point directly to A".
	// This means if B -> A, and we link A -> X, then B must point to X.
	_, err = tx.ExecContext(ctx,
		"UPDATE entity_aliases SET canonical_entity_id = $1, updated_at = now() WHERE canonical_entity_id = $2 AND ontology_version_id = $3",
		rootID, aliasEntityID, s.version.ID)
	if err != nil {
		return EntityAlias{}, fmt.Errorf("reparenting aliases: %w", err)
	}

	// 6. Create or update alias link for aliasEntityID -> rootID
	if metadata == nil {
		metadata = json.RawMessage("{}")
	}
	now := time.Now()
	var ea EntityAlias
	ea.ID = uuid.New().String()
	ea.AliasEntityID = aliasEntityID
	ea.CanonicalEntityID = rootID
	ea.Metadata = metadata
	ea.OntologyVersionID = s.version.ID

	err = tx.QueryRowContext(ctx,
		`INSERT INTO entity_aliases (id, ontology_version_id, alias_entity_id, canonical_entity_id, metadata, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (ontology_version_id, alias_entity_id) DO UPDATE 
		 SET canonical_entity_id = EXCLUDED.canonical_entity_id, 
		     metadata = EXCLUDED.metadata,
		     updated_at = EXCLUDED.updated_at
		 RETURNING id, created_at, updated_at`,
		ea.ID, ea.OntologyVersionID, ea.AliasEntityID, ea.CanonicalEntityID, ea.Metadata, now, now).Scan(&ea.ID, &ea.CreatedAt, &ea.UpdatedAt)
	if err != nil {
		return EntityAlias{}, fmt.Errorf("upserting alias: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return EntityAlias{}, err
	}

	return ea, nil
}

// GetCanonicalEntityID returns the canonical root ID for an entity.
func (s *Session) GetCanonicalEntityID(ctx context.Context, entityID string) (string, error) {
	var canonicalID string
	err := s.db.sqlDB.QueryRowContext(ctx,
		"SELECT canonical_entity_id FROM entity_aliases WHERE alias_entity_id = $1 AND ontology_version_id = $2",
		entityID, s.version.ID).Scan(&canonicalID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return entityID, nil
		}
		return "", err
	}
	return canonicalID, nil
}

func (s *Session) getCanonicalEntityIDTx(ctx context.Context, tx *sql.Tx, entityID string) (string, error) {
	var canonicalID string
	err := tx.QueryRowContext(ctx,
		"SELECT canonical_entity_id FROM entity_aliases WHERE alias_entity_id = $1 AND ontology_version_id = $2",
		entityID, s.version.ID).Scan(&canonicalID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return entityID, nil
		}
		return "", err
	}
	return canonicalID, nil
}

// GetAliasGroupEntityIDs returns all entity IDs in the alias group.
func (s *Session) GetAliasGroupEntityIDs(ctx context.Context, entityID string) ([]string, string, error) {
	canonicalID, err := s.GetCanonicalEntityID(ctx, entityID)
	if err != nil {
		return nil, "", err
	}

	rows, err := s.db.sqlDB.QueryContext(ctx,
		"SELECT alias_entity_id FROM entity_aliases WHERE canonical_entity_id = $1 AND ontology_version_id = $2",
		canonicalID, s.version.ID)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	group := []string{canonicalID}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, "", err
		}
		group = append(group, id)
	}
	return group, canonicalID, nil
}

// MergeEntityMetadata merges metadata from multiple entities, with canonical winning on conflict.
func MergeEntityMetadata(canonical json.RawMessage, aliases ...json.RawMessage) (json.RawMessage, error) {
	var dest map[string]interface{}
	if len(canonical) > 0 {
		if err := json.Unmarshal(canonical, &dest); err != nil {
			return nil, fmt.Errorf("unmarshaling canonical metadata: %w", err)
		}
	}
	if dest == nil {
		dest = make(map[string]interface{})
	}

	for _, a := range aliases {
		if len(a) == 0 {
			continue
		}
		var src map[string]interface{}
		if err := json.Unmarshal(a, &src); err != nil {
			return nil, fmt.Errorf("unmarshaling alias metadata: %w", err)
		}
		mergeMaps(dest, src)
	}

	return json.Marshal(dest)
}

func mergeMaps(dest, src map[string]interface{}) {
	for k, v := range src {
		if existing, ok := dest[k]; ok {
			destMap, destIsMap := existing.(map[string]interface{})
			srcMap, srcIsMap := v.(map[string]interface{})
			if destIsMap && srcIsMap {
				mergeMaps(destMap, srcMap)
			}
			// if types differ or not both maps, dest (canonical) wins
		} else {
			dest[k] = v
		}
	}
}
