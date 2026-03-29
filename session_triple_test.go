package acropora

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testTriple(t *testing.T, ctx context.Context, session *Session, db *DB, sqlDB *sql.DB, version *OntologyVersion) {
	t.Run("Triple insert with metadata", func(t *testing.T) {
		person, err := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "Alice Metadata"})
		require.NoError(t, err)
		company, err := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Company"}, RawName: "ACME Metadata"})
		require.NoError(t, err)

		from := time.Now().Truncate(time.Second)
		p, err := session.InsertPredicate(ctx, Predicate{
			PredicateDefinition: PredicateDefinition{
				Type: "works_at",
			},
			ValidFrom: from,
		})
		require.NoError(t, err)
		assert.True(t, p.ValidFrom.Equal(from))

		triple, err := session.InsertTriple(ctx, Triple{
			SubjectEntityID: person.ID,
			PredicateID:     p.ID,
			ObjectEntityID:  company.ID,
		})
		require.NoError(t, err)
		assert.Equal(t, p.ID, triple.PredicateID)

		fetchedP, err := session.GetPredicateByID(ctx, p.ID)
		require.NoError(t, err)
		assert.True(t, fetchedP.ValidFrom.Equal(from))
	})

	t.Run("TripleDefinition insert and read", func(t *testing.T) {
		// Create entities and predicate
		person, err := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "Alice"})
		require.NoError(t, err)
		company, err := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Company"}, RawName: "ACME"})
		require.NoError(t, err)
		worksAt, err := session.InsertPredicate(ctx, Predicate{PredicateDefinition: PredicateDefinition{Type: "works_at"}})
		require.NoError(t, err)

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
		person, err := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "Bob"})
		require.NoError(t, err)
		// Try to make Person works_at Person (not allowed by ontology)
		triple := Triple{
			SubjectEntityID: person.ID,
			PredicateID:     "non-existent-predicate", // non-existent predicate first
			ObjectEntityID:  person.ID,
		}
		_, err = session.InsertTriple(ctx, triple)
		assert.Error(t, err)

		// Now with valid runtime rows but invalid semantic
		worksAt, err := session.InsertPredicate(ctx, Predicate{PredicateDefinition: PredicateDefinition{Type: "works_at"}})
		require.NoError(t, err)
		triple.PredicateID = worksAt.ID
		_, err = session.InsertTriple(ctx, triple)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed by ontology")
	})
}
