package acropora

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testPredicate(t *testing.T, ctx context.Context, session *Session, db *DB, sqlDB *sql.DB, version *OntologyVersion) {
	t.Run("Predicate insert and read", func(t *testing.T) {
		now := time.Now().Truncate(time.Second)
		p := Predicate{
			PredicateDefinition: PredicateDefinition{
				Type:     "works_at",
				Metadata: json.RawMessage(`{"ontology_meta": "foo"}`),
			},
			ValidFrom: now,
			Metadata:  json.RawMessage(`{"runtime_meta": "bar"}`),
		}
		inserted, err := session.InsertPredicate(ctx, p)
		require.NoError(t, err)
		assert.NotEmpty(t, inserted.ID)
		assert.Equal(t, version.ID, inserted.OntologyVersionID)
		assert.Equal(t, "works_at", inserted.Type)
		assert.True(t, inserted.ValidFrom.Equal(now))

		fetched, err := session.GetPredicateByID(ctx, inserted.ID)
		require.NoError(t, err)
		assert.Equal(t, inserted.ID, fetched.ID)
		assert.Equal(t, "works_at", fetched.Type)
		assert.JSONEq(t, `{"runtime_meta": "bar"}`, string(fetched.Metadata))
	})

	t.Run("Predicate validation failure", func(t *testing.T) {
		p := Predicate{
			PredicateDefinition: PredicateDefinition{
				Type: "invalid_predicate",
			},
		}
		_, err := session.InsertPredicate(ctx, p)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed by ontology")
	})

	t.Run("ResolveOrInsertPredicate", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		p1 := Predicate{
			PredicateDefinition: PredicateDefinition{
				Type: "works_at",
			},
			ValidFrom: now,
			Metadata:  json.RawMessage(`{"m": 1}`),
		}

		// 1. Valid insert
		res1, err := session.ResolveOrInsertPredicate(ctx, p1)
		require.NoError(t, err)
		assert.NotEmpty(t, res1.ID)

		// 2. Reuse existing
		res2, err := session.ResolveOrInsertPredicate(ctx, p1)
		require.NoError(t, err)
		assert.Equal(t, res1.ID, res2.ID, "Should reuse existing predicate")

		// 3. Different validity window
		p2 := p1
		p2.ValidTo = now.Add(time.Hour)
		res3, err := session.ResolveOrInsertPredicate(ctx, p2)
		require.NoError(t, err)
		assert.NotEqual(t, res1.ID, res3.ID, "Should create new predicate for different validity window")

		// 4. Ontology validation failure
		p3 := Predicate{PredicateDefinition: PredicateDefinition{Type: "non_existent"}}
		_, err = session.ResolveOrInsertPredicate(ctx, p3)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed by ontology")

		// 5. Invalid temporal range
		p4 := Predicate{
			PredicateDefinition: PredicateDefinition{Type: "works_at"},
			ValidFrom:           now.Add(time.Hour),
			ValidTo:             now,
		}
		_, err = session.ResolveOrInsertPredicate(ctx, p4)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ValidTo cannot be before ValidFrom")

		// 6. Metadata persistence
		p5 := Predicate{
			PredicateDefinition: PredicateDefinition{Type: "works_at"},
			ValidFrom:           now.Add(2 * time.Hour),
			Metadata:            json.RawMessage(`{"special": "data"}`),
		}
		res5, err := session.ResolveOrInsertPredicate(ctx, p5)
		require.NoError(t, err)
		assert.JSONEq(t, `{"special": "data"}`, string(res5.Metadata))

		// 7. Different metadata should NOT reuse
		p6 := p5
		p6.Metadata = json.RawMessage(`{"special": "other"}`)
		res6, err := session.ResolveOrInsertPredicate(ctx, p6)
		require.NoError(t, err)
		assert.NotEqual(t, res5.ID, res6.ID, "Should create new predicate for different metadata")
	})
}
