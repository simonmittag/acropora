package acropora

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSession(t *testing.T) {
	ctx := context.Background()
	sqlDB, cleanup := setupTestDB(t)
	defer cleanup()

	db, err := New(ctx, sqlDB)
	require.NoError(t, err)

	// Ensure a clean slate for TestSession
	_, _ = sqlDB.ExecContext(ctx, "TRUNCATE TABLE triples, predicates, entities, entity_aliases, ontology_triples, ontology_predicates, ontology_entities, ontology_versions RESTART IDENTITY CASCADE")

	// 1. Seed ontology
	entities := []EntityDefinition{
		{Type: "Person"},
		{Type: "Company"},
		{Type: "Dummy"},
	}
	predicates := []PredicateDefinition{
		{Type: "works_at"},
		{Type: "knows"},
	}
	def := Definition{
		Entities:   entities,
		Predicates: predicates,
		Triples: []TripleDefinition{
			{
				Subject:   &entities[0],   // Person
				Predicate: &predicates[0], // works_at
				Object:    &entities[1],   // Company
			},
			{
				Subject:   &entities[0],   // Person
				Predicate: &predicates[1], // knows
				Object:    &entities[0],   // Person
			},
		},
	}

	version, err := db.SeedOntology(ctx, sqlDB, def, SeedOptions{Slug: "v1"})
	require.NoError(t, err)

	// 2. Create Session
	session := db.NewSession(version)

	testEntity(t, ctx, session, db, sqlDB, &version)
	testPredicate(t, ctx, session, db, sqlDB, &version)
	testTriple(t, ctx, session, db, sqlDB, &version)
}
