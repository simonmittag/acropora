package acropora

import (
	"encoding/json"
	"time"
)

// Version is the current version of the acropora library.
// It is injected at build time via ldflags.
var Version = "dev"

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
	Type     string
	Metadata json.RawMessage
}

// TripleDefinition represents the core properties of a triple definition.
type TripleDefinition struct {
	Subject   EntityDefinition
	Predicate PredicateDefinition
	Object    EntityDefinition
}

// Definition is a high-level ontology definition.
type Definition struct {
	Entities   []EntityDefinition
	Predicates []PredicateDefinition
	Triples    []TripleDefinition
}

// --- Persisted Types ---

// Direction represents the direction of a relationship.
type Direction string

const (
	DirectionIncoming Direction = "incoming"
	DirectionOutgoing Direction = "outgoing"
)

// Neighbour represents a flattened read model for a neighbouring entity.
type Neighbour struct {
	TripleID           string
	Direction          Direction
	PredicateType      string
	PredicateMetadata  json.RawMessage
	PredicateValidFrom time.Time
	PredicateValidTo   time.Time
	EntityID           string
	EntityType         string
	CanonicalName      string
	Metadata           json.RawMessage
}

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
	RawName       string
	CanonicalName string
}

// Predicate represents a persisted predicate (either Ontology or Runtime).
type Predicate struct {
	Persistable
	PredicateDefinition
	ValidFrom time.Time
	ValidTo   time.Time
	Metadata  json.RawMessage
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
