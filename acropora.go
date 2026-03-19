package acropora

import (
	"encoding/json"
	"time"
)

// OntologyVersion
type OntologyVersion struct {
	ID        string
	Hash      string
	CreatedAt time.Time
	UpdatedAt time.Time
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

// Entity
type Entity struct {
	Name     string
	Metadata json.RawMessage
}

// Predicate
type Predicate struct {
	Name      string
	ValidFrom *time.Time
	ValidTo   *time.Time
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
