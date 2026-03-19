package acropora

import (
	"encoding/json"
	"time"
)

// Entity
type Entity struct {
	ID                string
	OntologyVersionID string
	Name              string
	Metadata          json.RawMessage
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// Predicate
type Predicate struct {
	ID                string
	OntologyVersionID string
	Name              string
	ValidFrom         *time.Time
	ValidTo           *time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// Triple is a triple made from Entity and Predicate.
type Triple struct {
	Subject   Entity
	Predicate Predicate
	Object    Entity
}
