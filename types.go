package acropora

import (
	"encoding/json"
	"time"
)

// --- Base Types ---

// Persistable contains common fields for all database-backed entities.
type Persistable struct {
	ID                string
	OntologyVersionID string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// EntityDefinition represents the core properties of an entity.
type EntityDefinition struct {
	Type     string
	Metadata json.RawMessage
}

// PredicateDefinition represents the core properties of a predicate.
type PredicateDefinition struct {
	Type      string
	ValidFrom time.Time
	ValidTo   time.Time
}

// TripleDefinition represents the core properties of a triple definition.
type TripleDefinition struct {
	Subject   *EntityDefinition
	Predicate *PredicateDefinition
	Object    *EntityDefinition
}

// Definition is a high-level ontology definition.
type Definition struct {
	Entities   []EntityDefinition
	Predicates []PredicateDefinition
	Triples    []TripleDefinition
}

// --- Persisted Types ---

// OntologyVersion represents a specific version of the ontology.
type OntologyVersion struct {
	Persistable
	Slug string
	Hash string
}

// Entity represents a persisted entity (either Ontology or Runtime).
type Entity struct {
	Persistable
	EntityDefinition
}

// Predicate represents a persisted predicate (either Ontology or Runtime).
type Predicate struct {
	Persistable
	PredicateDefinition
}

// Triple represents a persisted triple (either Ontology or Runtime).
type Triple struct {
	Persistable
	SubjectEntityID string
	PredicateID     string
	ObjectEntityID  string
}

// EntityAlias represents a runtime link between two entities.
type EntityAlias struct {
	Persistable
	AliasEntityID     string
	CanonicalEntityID string
	Metadata          json.RawMessage
}
