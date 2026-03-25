package acropora

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRuntimeSession(t *testing.T) {
	ctx := context.Background()
	sqlDB, cleanup := setupTestDB(t)
	defer cleanup()

	db, err := New(ctx, sqlDB)
	require.NoError(t, err)

	// 1. Seed ontology
	entities := []EntityDefinition{
		{Name: "Person"},
		{Name: "Company"},
	}
	predicates := []PredicateDefinition{
		{Name: "works_at"},
	}
	def := Definition{
		Entities:   entities,
		Predicates: predicates,
		Triples: []TripleDefinition{
			{
				Subject:   &entities[0],
				Predicate: &predicates[0],
				Object:    &entities[1],
			},
		},
	}

	version, err := db.SeedOntology(ctx, sqlDB, def, SeedOptions{Slug: "v1"})
	require.NoError(t, err)

	// 2. Create Session
	session := db.NewSession(version)

	t.Run("EntityDefinition insert and read", func(t *testing.T) {
		e := Entity{
			EntityDefinition: EntityDefinition{
				Name:     "Person",
				Metadata: json.RawMessage(`{"age": 30}`),
			},
		}
		inserted, err := session.InsertEntity(ctx, e)
		require.NoError(t, err)
		assert.NotEmpty(t, inserted.ID)
		assert.Equal(t, version.ID, inserted.OntologyVersionID)
		assert.Equal(t, "Person", inserted.Name)

		fetched, err := session.GetEntityByID(ctx, inserted.ID)
		require.NoError(t, err)
		assert.Equal(t, inserted.ID, fetched.ID)
		assert.Equal(t, inserted.Name, fetched.Name)
	})

	t.Run("EntityDefinition validation failure", func(t *testing.T) {
		e := Entity{
			EntityDefinition: EntityDefinition{
				Name: "InvalidEntity",
			},
		}
		_, err := session.InsertEntity(ctx, e)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed by ontology")
	})

	t.Run("PredicateDefinition insert and read", func(t *testing.T) {
		now := time.Now()
		p := Predicate{
			PredicateDefinition: PredicateDefinition{
				Name:      "works_at",
				ValidFrom: now,
			},
		}
		inserted, err := session.InsertPredicate(ctx, p)
		require.NoError(t, err)
		assert.NotEmpty(t, inserted.ID)
		assert.Equal(t, version.ID, inserted.OntologyVersionID)

		fetched, err := session.GetPredicateByID(ctx, inserted.ID)
		require.NoError(t, err)
		assert.Equal(t, inserted.ID, fetched.ID)
	})

	t.Run("PredicateDefinition validation failure", func(t *testing.T) {
		p := Predicate{
			PredicateDefinition: PredicateDefinition{
				Name: "invalid_predicate",
			},
		}
		_, err := session.InsertPredicate(ctx, p)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed by ontology")
	})

	t.Run("TripleDefinition insert and read", func(t *testing.T) {
		// Create entities and predicate
		person, _ := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Name: "Person"}})
		company, _ := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Name: "Company"}})
		worksAt, _ := session.InsertPredicate(ctx, Predicate{PredicateDefinition: PredicateDefinition{Name: "works_at"}})

		triple := Triple{
			SubjectEntityID: person.ID,
			PredicateID:     worksAt.ID,
			ObjectEntityID:  company.ID,
		}

		inserted, err := session.InsertTriple(ctx, triple)
		require.NoError(t, err)
		assert.NotEmpty(t, inserted.ID)

		fetched, err := session.GetTripleByID(ctx, inserted.ID)
		require.NoError(t, err)
		assert.Equal(t, inserted.ID, fetched.ID)

		// Outgoing query
		outgoing, err := session.GetOutgoingTriples(ctx, person.ID)
		require.NoError(t, err)
		assert.Len(t, outgoing, 1)
		assert.Equal(t, inserted.ID, outgoing[0].ID)
	})

	t.Run("TripleDefinition validation failure - semantic", func(t *testing.T) {
		person, _ := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Name: "Person"}})
		// Try to make Person works_at Person (not allowed by ontology)
		triple := Triple{
			SubjectEntityID: person.ID,
			PredicateID:     "non-existent-predicate", // non-existent predicate first
			ObjectEntityID:  person.ID,
		}
		_, err := session.InsertTriple(ctx, triple)
		assert.Error(t, err)

		// Now with valid runtime rows but invalid semantic
		worksAt, _ := session.InsertPredicate(ctx, Predicate{PredicateDefinition: PredicateDefinition{Name: "works_at"}})
		triple.PredicateID = worksAt.ID
		_, err = session.InsertTriple(ctx, triple)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed by ontology")
	})
}
