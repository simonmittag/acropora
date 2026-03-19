package acropora

// Entity
type Entity struct {
	Name string
}

// Predicate
type Predicate struct {
	Name string
}

// Triple is a triple made from Entity and Predicate.
type Triple struct {
	Subject   Entity
	Predicate Predicate
	Object    Entity
}
