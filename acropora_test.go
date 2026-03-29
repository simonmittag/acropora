package acropora

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
	}
	predicates := []PredicateDefinition{
		{Type: "works_at"},
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
				Type:     "Person",
				Metadata: json.RawMessage(`{"age": 30}`),
			},
			RawName: "  John   Doe  ",
		}
		inserted, err := session.InsertEntity(ctx, e)
		require.NoError(t, err)
		assert.NotEmpty(t, inserted.ID)
		assert.Equal(t, version.ID, inserted.OntologyVersionID)
		assert.Equal(t, "Person", inserted.Type)
		assert.Equal(t, "  John   Doe  ", inserted.RawName)
		assert.Equal(t, "john doe", inserted.CanonicalName)

		fetched, err := session.GetEntityByID(ctx, inserted.ID)
		require.NoError(t, err)
		assert.Equal(t, inserted.ID, fetched.ID)
		assert.Equal(t, inserted.Type, fetched.Type)
		assert.Equal(t, inserted.RawName, fetched.RawName)
		assert.Equal(t, inserted.CanonicalName, fetched.CanonicalName)
	})

	t.Run("EntityDefinition validation failure", func(t *testing.T) {
		e := Entity{
			EntityDefinition: EntityDefinition{
				Type: "InvalidEntity",
			},
			RawName: "Some Name",
		}
		_, err := session.InsertEntity(ctx, e)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed by ontology")
	})

	t.Run("Conservative canonicalization", func(t *testing.T) {
		tests := []struct {
			input    string
			expected string
			name     string
		}{
			{"  Leading Trailing  ", "leading trailing", "trim whitespace"},
			{"Multiple    Spaces", "multiple spaces", "collapse whitespace"},
			{"Mixed CASE", "mixed case", "lowercase"},
			{"Pty Ltd", "pty ltd", "preserve legal suffixes"},
			{"Special Char", "special char", "remove non-printable"},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				e := Entity{
					EntityDefinition: EntityDefinition{Type: "Person"},
					RawName:          tc.input,
				}
				inserted, err := session.InsertEntity(ctx, e)
				require.NoError(t, err)
				assert.Equal(t, tc.expected, inserted.CanonicalName)
			})
		}
	})

	t.Run("Uniqueness", func(t *testing.T) {
		e1 := Entity{
			EntityDefinition: EntityDefinition{Type: "Person"},
			RawName:          "Unique Name",
		}
		_, err := session.InsertEntity(ctx, e1)
		require.NoError(t, err)

		e2 := Entity{
			EntityDefinition: EntityDefinition{Type: "Person"},
			RawName:          "unique name", // Different raw, same canonical
		}
		_, err = session.InsertEntity(ctx, e2)
		assert.Error(t, err, "Should fail due to unique constraint on canonical name")

		// Same canonical name in different ontology version should pass
		def2 := Definition{
			Entities: []EntityDefinition{{Type: "Person"}},
		}
		version2, err := db.SeedOntology(ctx, sqlDB, def2, SeedOptions{Slug: "v2"})
		require.NoError(t, err)
		session2 := db.NewSession(version2)

		_, err = session2.InsertEntity(ctx, e1)
		assert.NoError(t, err, "Should allow same canonical name in different ontology version")
	})

	t.Run("Lookup by raw name", func(t *testing.T) {
		raw := "  Lookup   Me  "
		e := Entity{
			EntityDefinition: EntityDefinition{Type: "Person"},
			RawName:          raw,
		}
		inserted, err := session.InsertEntity(ctx, e)
		require.NoError(t, err)

		// Exact match
		fetched, err := session.GetEntityByRawName(ctx, raw)
		require.NoError(t, err)
		assert.Equal(t, inserted.ID, fetched.ID)

		// Case/spacing variant
		fetched, err = session.GetEntityByRawName(ctx, "lookup me")
		require.NoError(t, err)
		assert.Equal(t, inserted.ID, fetched.ID)

		fetched, err = session.GetEntityByRawName(ctx, "  LOOKUP me  ")
		require.NoError(t, err)
		assert.Equal(t, inserted.ID, fetched.ID)
	})

	t.Run("Alias-aware lookup", func(t *testing.T) {
		canonical, err := session.InsertEntity(ctx, Entity{
			EntityDefinition: EntityDefinition{Type: "Company"},
			RawName:          "Canonical Corp",
		})
		require.NoError(t, err)

		alias, err := session.InsertEntity(ctx, Entity{
			EntityDefinition: EntityDefinition{Type: "Company"},
			RawName:          "Alias Inc",
		})
		require.NoError(t, err)

		_, err = session.LinkEntityAlias(ctx, alias.ID, canonical.ID, nil)
		require.NoError(t, err)

		// Lookup by alias raw name should return canonical entity
		fetched, err := session.GetEntityByRawName(ctx, "Alias Inc")
		require.NoError(t, err)
		assert.Equal(t, canonical.ID, fetched.ID)
		assert.Equal(t, "Canonical Corp", fetched.RawName)
	})

	t.Run("PredicateDefinition insert and read", func(t *testing.T) {
		now := time.Now()
		p := Predicate{
			PredicateDefinition: PredicateDefinition{
				Type:      "works_at",
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
				Type: "invalid_predicate",
			},
		}
		_, err := session.InsertPredicate(ctx, p)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed by ontology")
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

	t.Run("Entity Aliasing", func(t *testing.T) {
		// Setup
		a, err := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "A"})
		require.NoError(t, err)
		b, err := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "B"})
		require.NoError(t, err)
		c, err := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Company"}, RawName: "C"})
		require.NoError(t, err)
		worksAt, err := session.InsertPredicate(ctx, Predicate{PredicateDefinition: PredicateDefinition{Type: "works_at"}})
		require.NoError(t, err)

		// Test A: Simple alias link
		_, err = session.LinkEntityAlias(ctx, b.ID, a.ID, json.RawMessage(`{"source": "merger"}`))
		require.NoError(t, err)

		canonicalA, err := session.GetCanonicalEntityID(ctx, a.ID)
		require.NoError(t, err)
		assert.Equal(t, a.ID, canonicalA)

		canonicalB, err := session.GetCanonicalEntityID(ctx, b.ID)
		require.NoError(t, err)
		assert.Equal(t, a.ID, canonicalB)

		group, root, err := session.GetAliasGroupEntityIDs(ctx, a.ID)
		require.NoError(t, err)
		assert.Equal(t, a.ID, root)
		assert.ElementsMatch(t, []string{a.ID, b.ID}, group)

		// Test B: Triple query unification
		// A has C
		_, err = session.InsertTriple(ctx, Triple{SubjectEntityID: a.ID, PredicateID: worksAt.ID, ObjectEntityID: c.ID})
		require.NoError(t, err)
		// B already linked to A. Triples on B stay on B.
		// Let's insert one on B directly into DB to simulate historical triple if needed,
		// but the instruction says "old triples on B stay on B physically".
		// To test this we need a triple that WAS on B before it was linked, OR just insert it bypass canonicalization if we want to be sure.
		// Actually, let's just use the Session to insert it BEFORE linking.

		d, err := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Company"}, RawName: "D"})
		require.NoError(t, err)
		b2, err := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "B2"})
		require.NoError(t, err)
		_, err = session.InsertTriple(ctx, Triple{SubjectEntityID: b2.ID, PredicateID: worksAt.ID, ObjectEntityID: d.ID})
		require.NoError(t, err)

		// Link b2 -> a
		_, err = session.LinkEntityAlias(ctx, b2.ID, a.ID, nil)
		require.NoError(t, err)

		// Query from A should return both
		outgoingA, err := session.GetOutgoingTriples(ctx, a.ID)
		require.NoError(t, err)
		assert.Len(t, outgoingA, 2)
		for _, tr := range outgoingA {
			assert.Equal(t, a.ID, tr.SubjectEntityID)
		}

		// Query from B2 should return same
		outgoingB2, err := session.GetOutgoingTriples(ctx, b2.ID)
		require.NoError(t, err)
		assert.Len(t, outgoingB2, 2)
		assert.Equal(t, outgoingA, outgoingB2)

		// Test C: New writes via alias canonicalize
		e, err := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Company"}, RawName: "E"})
		require.NoError(t, err)
		tr, err := session.InsertTriple(ctx, Triple{SubjectEntityID: b.ID, PredicateID: worksAt.ID, ObjectEntityID: e.ID})
		require.NoError(t, err)
		assert.Equal(t, a.ID, tr.SubjectEntityID)

		// Test D: Child alias reparenting
		// D -> B, then B -> A
		entD, err := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "D_ent"})
		require.NoError(t, err)
		entB, err := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "B_ent"})
		require.NoError(t, err)
		entA, err := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "A_ent"})
		require.NoError(t, err)

		_, err = session.LinkEntityAlias(ctx, entD.ID, entB.ID, nil)
		require.NoError(t, err)
		_, err = session.LinkEntityAlias(ctx, entB.ID, entA.ID, nil)
		require.NoError(t, err)

		// Check entD now points to entA
		canD, err := session.GetCanonicalEntityID(ctx, entD.ID)
		require.NoError(t, err)
		assert.Equal(t, entA.ID, canD)

		groupA, rootA, err := session.GetAliasGroupEntityIDs(ctx, entA.ID)
		require.NoError(t, err)
		assert.Equal(t, entA.ID, rootA)
		assert.ElementsMatch(t, []string{entA.ID, entB.ID, entD.ID}, groupA)

		// Test E: Cycle rejection
		// B -> A already exists from before (b -> a)
		_, err = session.LinkEntityAlias(ctx, a.ID, b.ID, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already in the same alias group")

		// Test F: Cross-version rejection
		// Create a slightly different definition to avoid hash collision
		def2 := Definition{
			Entities:   append([]EntityDefinition{{Type: "Dummy"}}, def.Entities...),
			Predicates: def.Predicates,
		}
		// Re-point triples to the new entities slice to pass pointer validation in SeedOntology
		def2.Triples = []TripleDefinition{
			{
				Subject:   &def2.Entities[1],   // "Person"
				Predicate: &def2.Predicates[0], // "works_at"
				Object:    &def2.Entities[2],   // "Company"
			},
		}

		version2, err := db.SeedOntology(ctx, sqlDB, def2, SeedOptions{Slug: "v2-aliasing"})
		require.NoError(t, err)
		session2 := db.NewSession(version2)
		entV2, err := session2.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "V2_Person"})
		require.NoError(t, err)

		_, err = session.LinkEntityAlias(ctx, entV2.ID, a.ID, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
		assert.Contains(t, err.Error(), "ontology version")
	})

	t.Run("Metadata Merge", func(t *testing.T) {
		canonical := json.RawMessage(`{"name": "Alice", "details": {"age": 30, "city": "London"}, "tags": ["a"]}`)
		alias1 := json.RawMessage(`{"name": "Bob", "details": {"age": 25, "job": "Engineer"}, "tags": ["b"], "extra": "foo"}`)

		merged, err := MergeEntityMetadata(canonical, alias1)
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal(merged, &result)
		require.NoError(t, err)

		assert.Equal(t, "Alice", result["name"]) // Canonical wins
		assert.Equal(t, "foo", result["extra"])  // Alias fills missing

		details := result["details"].(map[string]interface{})
		assert.Equal(t, float64(30), details["age"]) // Canonical wins in nested
		assert.Equal(t, "London", details["city"])   // Canonical wins in nested
		assert.Equal(t, "Engineer", details["job"])  // Alias fills missing in nested

		assert.Equal(t, []interface{}{"a"}, result["tags"]) // Arrays are not merged, canonical wins
	})
}
