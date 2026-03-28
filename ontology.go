package acropora

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/lucasepe/codename"
)

// SeedOptions provides configuration for seeding an ontology.
type SeedOptions struct {
	Slug string
}

// GetOntologyVersionOptions provides configuration for fetching an ontology version.
type GetOntologyVersionOptions map[string]string

const (
	OptionID   = "id"
	OptionHash = "hash"
	OptionSlug = "slug"
)

// OntologySeeder is the interface for seeding an ontology.
type OntologySeeder interface {
	SeedOntology(ctx context.Context, db *sql.DB, def Definition, opts SeedOptions) (OntologyVersion, error)
	ListOntologyVersions(ctx context.Context) ([]OntologyVersion, error)
	GetOntologyVersion(ctx context.Context, opts GetOntologyVersionOptions) (OntologyVersion, error)
}

// ListOntologyVersions returns all ontology versions, sorted by most recent first.
func (d *DB) ListOntologyVersions(ctx context.Context) ([]OntologyVersion, error) {
	rows, err := d.sqlDB.QueryContext(ctx, "SELECT id, slug, hash, created_at, updated_at FROM ontology_versions ORDER BY created_at DESC")
	if err != nil {
		return nil, fmt.Errorf("querying ontology versions: %w", err)
	}
	defer rows.Close()

	var versions []OntologyVersion
	for rows.Next() {
		var v OntologyVersion
		if err := rows.Scan(&v.ID, &v.Slug, &v.Hash, &v.CreatedAt, &v.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scanning ontology version: %w", err)
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return versions, nil
}

// GetOntologyVersion returns an ontology version based on the provided options.
// If options are empty, it returns the latest (default) version.
// Supported keys: id, hash, slug. Only one key can be provided at a time.
func (d *DB) GetOntologyVersion(ctx context.Context, opts GetOntologyVersionOptions) (OntologyVersion, error) {
	if len(opts) > 1 {
		return OntologyVersion{}, fmt.Errorf("only one filter option allowed (id, hash, or slug)")
	}

	query := "SELECT id, slug, hash, created_at, updated_at FROM ontology_versions"
	var arg any

	if len(opts) == 0 {
		query += " ORDER BY created_at DESC LIMIT 1"
	} else {
		for k, v := range opts {
			switch k {
			case OptionID:
				query += " WHERE id = $1"
				arg = v
			case OptionHash:
				query += " WHERE hash = $1"
				arg = v
			case OptionSlug:
				query += " WHERE slug = $1"
				arg = v
			default:
				return OntologyVersion{}, fmt.Errorf("unsupported filter option: %s", k)
			}
		}
	}

	var v OntologyVersion
	var err error
	if arg != nil {
		err = d.sqlDB.QueryRowContext(ctx, query, arg).Scan(&v.ID, &v.Slug, &v.Hash, &v.CreatedAt, &v.UpdatedAt)
	} else {
		err = d.sqlDB.QueryRowContext(ctx, query).Scan(&v.ID, &v.Slug, &v.Hash, &v.CreatedAt, &v.UpdatedAt)
	}

	if err != nil {
		if err == sql.ErrNoRows {
			if len(opts) == 0 {
				return OntologyVersion{}, fmt.Errorf("no ontology versions found")
			}
			return OntologyVersion{}, fmt.Errorf("ontology version not found")
		}
		return OntologyVersion{}, fmt.Errorf("querying ontology version: %w", err)
	}
	return v, nil
}

// SeedOntology validates and writes a new ontology version to the database.
func (d *DB) SeedOntology(ctx context.Context, db *sql.DB, def Definition, opts SeedOptions) (OntologyVersion, error) {
	// 1. Validation
	if err := validateOntologyDefinition(def); err != nil {
		return OntologyVersion{}, fmt.Errorf("validation failed: %w", err)
	}

	// 2. Compute deterministic hash
	hash, err := computeOntologyHash(def)
	if err != nil {
		return OntologyVersion{}, fmt.Errorf("failed to compute hash: %w", err)
	}

	// 3. Database transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return OntologyVersion{}, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// 4. Handle slug
	var slug string
	if opts.Slug != "" {
		slug = opts.Slug
	} else {
		// Generate slug with retries
		rng, err := codename.DefaultRNG()
		if err != nil {
			return OntologyVersion{}, fmt.Errorf("failed to get RNG: %w", err)
		}

		maxRetries := 50
		for i := 0; i < maxRetries; i++ {
			candidate := codename.Generate(rng, 0)
			var exists bool
			err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM ontology_versions WHERE slug = $1)", candidate).Scan(&exists)
			if err != nil {
				return OntologyVersion{}, fmt.Errorf("failed to check slug existence: %w", err)
			}
			if !exists {
				slug = candidate
				break
			}
			if i == maxRetries-1 {
				return OntologyVersion{}, fmt.Errorf("failed to generate unique slug after %d attempts", maxRetries)
			}
		}
	}

	// Insert ontology version
	versionID := uuid.New().String()
	version := OntologyVersion{
		Persistable: Persistable{
			ID: versionID,
		},
		Slug: slug,
		Hash: hash,
	}

	err = tx.QueryRowContext(ctx,
		"INSERT INTO ontology_versions (id, hash, slug) VALUES ($1, $2, $3) RETURNING created_at, updated_at",
		version.ID, version.Hash, version.Slug).Scan(&version.CreatedAt, &version.UpdatedAt)
	if err != nil {
		return OntologyVersion{}, fmt.Errorf("failed to insert ontology version: %w", err)
	}

	// Insert entities
	entityToID := make(map[*EntityDefinition]string)
	for i := range def.Entities {
		eDef := &def.Entities[i]
		id := uuid.New().String()
		metadata := eDef.Metadata
		if metadata == nil {
			metadata = json.RawMessage("{}")
		}
		_, err = tx.ExecContext(ctx,
			"INSERT INTO ontology_entities (id, ontology_version_id, type, metadata) VALUES ($1, $2, $3, $4)",
			id, versionID, eDef.Type, metadata)
		if err != nil {
			return OntologyVersion{}, fmt.Errorf("failed to insert entity %s: %w", eDef.Type, err)
		}
		entityToID[eDef] = id
	}

	// Insert predicates
	predicateToID := make(map[*PredicateDefinition]string)
	for i := range def.Predicates {
		pDef := &def.Predicates[i]
		id := uuid.New().String()
		_, err = tx.ExecContext(ctx,
			"INSERT INTO ontology_predicates (id, ontology_version_id, type, valid_from, valid_to) VALUES ($1, $2, $3, $4, $5)",
			id, versionID, pDef.Type, pDef.ValidFrom, pDef.ValidTo)
		if err != nil {
			return OntologyVersion{}, fmt.Errorf("failed to insert predicate %s: %w", pDef.Type, err)
		}
		predicateToID[pDef] = id
	}

	// Insert triples
	for _, tDef := range def.Triples {
		id := uuid.New().String()
		subjectID := entityToID[tDef.Subject]
		predicateID := predicateToID[tDef.Predicate]
		objectID := entityToID[tDef.Object]

		triple := &Triple{
			Persistable: Persistable{
				ID:                id,
				OntologyVersionID: versionID,
			},
			SubjectEntityID: subjectID,
			PredicateID:     predicateID,
			ObjectEntityID:  objectID,
		}

		_, err = tx.ExecContext(ctx,
			"INSERT INTO ontology_triples (id, ontology_version_id, subject_entity_id, predicate_id, object_entity_id) VALUES ($1, $2, $3, $4, $5)",
			triple.ID, triple.OntologyVersionID, triple.SubjectEntityID, triple.PredicateID, triple.ObjectEntityID)
		if err != nil {
			return OntologyVersion{}, fmt.Errorf("failed to insert triple (%s, %s, %s): %w", tDef.Subject.Type, tDef.Predicate.Type, tDef.Object.Type, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return OntologyVersion{}, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return version, nil
}

func validateOntologyDefinition(def Definition) error {
	entityTypes := make(map[string]bool)
	entityPtrs := make(map[*EntityDefinition]bool)
	for i := range def.Entities {
		e := &def.Entities[i]
		if entityTypes[e.Type] {
			return fmt.Errorf("duplicate entity type: %s", e.Type)
		}
		entityTypes[e.Type] = true
		entityPtrs[e] = true
	}

	predicateTypes := make(map[string]bool)
	predicatePtrs := make(map[*PredicateDefinition]bool)
	for i := range def.Predicates {
		p := &def.Predicates[i]
		if predicateTypes[p.Type] {
			return fmt.Errorf("duplicate predicate type: %s", p.Type)
		}
		predicateTypes[p.Type] = true
		predicatePtrs[p] = true
	}

	for _, t := range def.Triples {
		if t.Subject == nil || !entityPtrs[t.Subject] {
			return fmt.Errorf("triple references non-existent subject entity")
		}
		if t.Predicate == nil || !predicatePtrs[t.Predicate] {
			return fmt.Errorf("triple references non-existent predicate")
		}
		if t.Object == nil || !entityPtrs[t.Object] {
			return fmt.Errorf("triple references non-existent object entity")
		}
	}

	return nil
}

func computeOntologyHash(def Definition) (string, error) {
	// To compute a stable hash with pointers, we transform it into a canonical representation using names
	type TripleCanonical struct {
		SubjectType   string
		PredicateType string
		ObjectType    string
	}

	type DefinitionCanonical struct {
		Entities   []EntityDefinition
		Predicates []PredicateDefinition
		Triples    []TripleCanonical
	}

	entities := make([]EntityDefinition, len(def.Entities))
	copy(entities, def.Entities)
	sort.Slice(entities, func(i, j int) bool {
		return entities[i].Type < entities[j].Type
	})

	predicates := make([]PredicateDefinition, len(def.Predicates))
	copy(predicates, def.Predicates)
	sort.Slice(predicates, func(i, j int) bool {
		return predicates[i].Type < predicates[j].Type
	})

	triples := make([]TripleCanonical, len(def.Triples))
	for i, t := range def.Triples {
		triples[i] = TripleCanonical{
			SubjectType:   t.Subject.Type,
			PredicateType: t.Predicate.Type,
			ObjectType:    t.Object.Type,
		}
	}
	sort.Slice(triples, func(i, j int) bool {
		if triples[i].SubjectType != triples[j].SubjectType {
			return triples[i].SubjectType < triples[j].SubjectType
		}
		if triples[i].PredicateType != triples[j].PredicateType {
			return triples[i].PredicateType < triples[j].PredicateType
		}
		return triples[i].ObjectType < triples[j].ObjectType
	})

	canonicalDef := DefinitionCanonical{
		Entities:   entities,
		Predicates: predicates,
		Triples:    triples,
	}

	data, err := json.Marshal(canonicalDef)
	if err != nil {
		return "", err
	}

	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}
