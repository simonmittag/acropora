package acropora

import (
	"context"
	"fmt"
	"testing"

	acropora_db "github.com/simonmittag/acropora/internal/db"
	"github.com/stretchr/testify/require"
)

func TestSession(t *testing.T) {
	ctx := context.Background()
	sqlDB, cleanup := setupTestDB(t)
	defer cleanup()

	tablePrefix := "acropora"
	db, err := New(ctx, sqlDB, Options{TablePrefix: tablePrefix})
	require.NoError(t, err)

	// Ensure a clean slate for TestSession
	_, _ = sqlDB.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s, %s, %s, %s, %s, %s, %s, %s RESTART IDENTITY CASCADE",
		acropora_db.TableName(tablePrefix, acropora_db.TableTriples),
		acropora_db.TableName(tablePrefix, acropora_db.TablePredicates),
		acropora_db.TableName(tablePrefix, acropora_db.TableEntities),
		acropora_db.TableName(tablePrefix, acropora_db.TableEntityAliases),
		acropora_db.TableName(tablePrefix, acropora_db.TableOntologyTriples),
		acropora_db.TableName(tablePrefix, acropora_db.TableOntologyPredicates),
		acropora_db.TableName(tablePrefix, acropora_db.TableOntologyEntities),
		acropora_db.TableName(tablePrefix, acropora_db.TableOntologyVersions)))

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
				Subject:   entities[0],   // Person
				Predicate: predicates[0], // works_at
				Object:    entities[1],   // Company
			},
			{
				Subject:   entities[0],   // Person
				Predicate: predicates[1], // knows
				Object:    entities[0],   // Person
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
