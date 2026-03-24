package acropora

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// Entity
type Entity struct {
	Name     string
	Metadata json.RawMessage
}

// Predicate
type Predicate struct {
	Name      string
	ValidFrom time.Time
	ValidTo   time.Time
}

// Triple
type Triple struct {
	Subject   *Entity
	Predicate *Predicate
	Object    *Entity
}

// Definition
type Definition struct {
	Entities   []Entity
	Predicates []Predicate
	Triples    []Triple
}

// OntologyEntity
type OntologyEntity struct {
	ID                string
	OntologyVersionID string
	Entity
	CreatedAt time.Time
	UpdatedAt time.Time
}

// OntologyPredicate
type OntologyPredicate struct {
	ID                string
	OntologyVersionID string
	Predicate
	CreatedAt time.Time
	UpdatedAt time.Time
}

// OntologyTriple
type OntologyTriple struct {
	ID                string
	OntologyVersionID string
	Triple
	SubjectEntityID string
	PredicateID     string
	ObjectEntityID  string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// OntologyVersion
type OntologyVersion struct {
	ID   string
	Hash string
	Date time.Time
}

// OntologySeeder is the interface for seeding an ontology.
type OntologySeeder interface {
	SeedOntology(ctx context.Context, db *sql.DB, def Definition) (OntologyVersion, error)
	ListOntologyVersions(ctx context.Context) ([]OntologyVersion, error)
	GetDefaultOntologyVersion(ctx context.Context) (OntologyVersion, error)
}

// ListOntologyVersions returns all ontology versions, sorted by most recent first.
func (d *DB) ListOntologyVersions(ctx context.Context) ([]OntologyVersion, error) {
	rows, err := d.sqlDB.QueryContext(ctx, "SELECT id, hash, created_at FROM ontology_versions ORDER BY created_at DESC")
	if err != nil {
		return nil, fmt.Errorf("querying ontology versions: %w", err)
	}
	defer rows.Close()

	var versions []OntologyVersion
	for rows.Next() {
		var v OntologyVersion
		if err := rows.Scan(&v.ID, &v.Hash, &v.Date); err != nil {
			return nil, fmt.Errorf("scanning ontology version: %w", err)
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return versions, nil
}

// GetDefaultOntologyVersion returns the latest ontology version.
func (d *DB) GetDefaultOntologyVersion(ctx context.Context) (OntologyVersion, error) {
	var v OntologyVersion
	err := d.sqlDB.QueryRowContext(ctx, "SELECT id, hash, created_at FROM ontology_versions ORDER BY created_at DESC LIMIT 1").Scan(&v.ID, &v.Hash, &v.Date)
	if err != nil {
		if err == sql.ErrNoRows {
			return OntologyVersion{}, fmt.Errorf("no ontology versions found")
		}
		return OntologyVersion{}, fmt.Errorf("querying latest ontology version: %w", err)
	}
	return v, nil
}

// SeedOntology validates and writes a new ontology version to the database.
func (d *DB) SeedOntology(ctx context.Context, db *sql.DB, def Definition) (OntologyVersion, error) {
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

	// Insert ontology version
	versionID := uuid.New().String()
	version := OntologyVersion{
		ID:   versionID,
		Hash: hash,
	}

	err = tx.QueryRowContext(ctx,
		"INSERT INTO ontology_versions (id, hash) VALUES ($1, $2) RETURNING created_at",
		version.ID, version.Hash).Scan(&version.Date)
	if err != nil {
		return OntologyVersion{}, fmt.Errorf("failed to insert ontology version: %w", err)
	}

	// Insert entities
	entityToID := make(map[*Entity]string)
	for i := range def.Entities {
		eDef := &def.Entities[i]
		id := uuid.New().String()
		metadata := eDef.Metadata
		if metadata == nil {
			metadata = json.RawMessage("{}")
		}
		_, err = tx.ExecContext(ctx,
			"INSERT INTO ontology_entities (id, ontology_version_id, name, metadata) VALUES ($1, $2, $3, $4)",
			id, versionID, eDef.Name, metadata)
		if err != nil {
			return OntologyVersion{}, fmt.Errorf("failed to insert entity %s: %w", eDef.Name, err)
		}
		entityToID[eDef] = id
	}

	// Insert predicates
	predicateToID := make(map[*Predicate]string)
	for i := range def.Predicates {
		pDef := &def.Predicates[i]
		id := uuid.New().String()
		_, err = tx.ExecContext(ctx,
			"INSERT INTO ontology_predicates (id, ontology_version_id, name, valid_from, valid_to) VALUES ($1, $2, $3, $4, $5)",
			id, versionID, pDef.Name, pDef.ValidFrom, pDef.ValidTo)
		if err != nil {
			return OntologyVersion{}, fmt.Errorf("failed to insert predicate %s: %w", pDef.Name, err)
		}
		predicateToID[pDef] = id
	}

	// Insert triples
	for _, tDef := range def.Triples {
		id := uuid.New().String()
		subjectID := entityToID[tDef.Subject]
		predicateID := predicateToID[tDef.Predicate]
		objectID := entityToID[tDef.Object]

		triple := &OntologyTriple{
			ID:                id,
			OntologyVersionID: versionID,
			Triple:            tDef,
			SubjectEntityID:   subjectID,
			PredicateID:       predicateID,
			ObjectEntityID:    objectID,
		}

		_, err = tx.ExecContext(ctx,
			"INSERT INTO ontology_triples (id, ontology_version_id, subject_entity_id, predicate_id, object_entity_id) VALUES ($1, $2, $3, $4, $5)",
			triple.ID, triple.OntologyVersionID, triple.SubjectEntityID, triple.PredicateID, triple.ObjectEntityID)
		if err != nil {
			return OntologyVersion{}, fmt.Errorf("failed to insert triple (%s, %s, %s): %w", triple.Subject.Name, triple.Predicate.Name, triple.Object.Name, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return OntologyVersion{}, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return version, nil
}

func validateOntologyDefinition(def Definition) error {
	entityNames := make(map[string]bool)
	entityPtrs := make(map[*Entity]bool)
	for i := range def.Entities {
		e := &def.Entities[i]
		if entityNames[e.Name] {
			return fmt.Errorf("duplicate entity name: %s", e.Name)
		}
		entityNames[e.Name] = true
		entityPtrs[e] = true
	}

	predicateNames := make(map[string]bool)
	predicatePtrs := make(map[*Predicate]bool)
	for i := range def.Predicates {
		p := &def.Predicates[i]
		if predicateNames[p.Name] {
			return fmt.Errorf("duplicate predicate name: %s", p.Name)
		}
		predicateNames[p.Name] = true
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
		SubjectName   string
		PredicateName string
		ObjectName    string
	}

	type DefinitionCanonical struct {
		Entities   []Entity
		Predicates []Predicate
		Triples    []TripleCanonical
	}

	entities := make([]Entity, len(def.Entities))
	copy(entities, def.Entities)
	sort.Slice(entities, func(i, j int) bool {
		return entities[i].Name < entities[j].Name
	})

	predicates := make([]Predicate, len(def.Predicates))
	copy(predicates, def.Predicates)
	sort.Slice(predicates, func(i, j int) bool {
		return predicates[i].Name < predicates[j].Name
	})

	triples := make([]TripleCanonical, len(def.Triples))
	for i, t := range def.Triples {
		triples[i] = TripleCanonical{
			SubjectName:   t.Subject.Name,
			PredicateName: t.Predicate.Name,
			ObjectName:    t.Object.Name,
		}
	}
	sort.Slice(triples, func(i, j int) bool {
		if triples[i].SubjectName != triples[j].SubjectName {
			return triples[i].SubjectName < triples[j].SubjectName
		}
		if triples[i].PredicateName != triples[j].PredicateName {
			return triples[i].PredicateName < triples[j].PredicateName
		}
		return triples[i].ObjectName < triples[j].ObjectName
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
