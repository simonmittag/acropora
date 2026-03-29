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

		canonicalA, err := session.GetAntiAliasedEntityID(ctx, a.ID)
		require.NoError(t, err)
		assert.Equal(t, a.ID, canonicalA)

		canonicalB, err := session.GetAntiAliasedEntityID(ctx, b.ID)
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
		canD, err := session.GetAntiAliasedEntityID(ctx, entD.ID)
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

	t.Run("ResolveOrInsertEntity", func(t *testing.T) {
		// Ensure a clean slate
		_, _ = sqlDB.ExecContext(ctx, "TRUNCATE TABLE triples, predicates, entities, entity_aliases, ontology_triples, ontology_predicates, ontology_entities, ontology_versions RESTART IDENTITY CASCADE")

		// 1. Seed ontology
		entitiesDef := []EntityDefinition{
			{Type: "Company"},
			{Type: "Person"},
		}
		def := Definition{
			Entities: entitiesDef,
		}

		version, err := db.SeedOntology(ctx, sqlDB, def, SeedOptions{Slug: "v-resolve"})
		require.NoError(t, err)

		session := db.NewSession(version)

		// Test A: Insert new entity
		e := Entity{
			EntityDefinition: EntityDefinition{Type: "Company"},
			RawName:          "Apple Inc.",
		}
		resolved, err := session.ResolveOrInsertEntity(ctx, e)
		require.NoError(t, err)
		assert.NotEmpty(t, resolved.ID)
		assert.Equal(t, "Apple Inc.", resolved.RawName)
		assert.Equal(t, "apple inc.", resolved.CanonicalName)

		// Test B: Resolve existing entity by canonical name
		e2 := Entity{
			EntityDefinition: EntityDefinition{Type: "Company"},
			RawName:          "  APPLE  INC.  ",
		}
		resolved2, err := session.ResolveOrInsertEntity(ctx, e2)
		require.NoError(t, err)
		assert.Equal(t, resolved.ID, resolved2.ID)
		assert.Equal(t, "Apple Inc.", resolved2.RawName)

		// Test C: Resolve through alias
		e3 := Entity{
			EntityDefinition: EntityDefinition{Type: "Company"},
			RawName:          "Apple",
		}
		inserted3, err := session.InsertEntity(ctx, e3)
		require.NoError(t, err)

		// Link "Apple" as an alias of "Apple Inc."
		_, err = session.LinkEntityAlias(ctx, inserted3.ID, resolved.ID, nil)
		require.NoError(t, err)

		// Now ResolveOrInsert "Apple" should return "Apple Inc."
		eResolve := Entity{
			EntityDefinition: EntityDefinition{Type: "Company"},
			RawName:          "Apple",
		}
		resolvedAlias, err := session.ResolveOrInsertEntity(ctx, eResolve)
		require.NoError(t, err)
		assert.Equal(t, resolved.ID, resolvedAlias.ID)
		assert.Equal(t, "Apple Inc.", resolvedAlias.RawName)

		// Test D: Different type with same name (if allowed by ontology, but here it's still same canonical name)
		// Our current implementation of GetEntityByRawName doesn't care about type, it just finds the entity by name.
		// If we wanted to support same name for different types, we'd need to change the schema/logic.
		// For now, it will resolve to the existing one regardless of type in the input.
		ePerson := Entity{
			EntityDefinition: EntityDefinition{Type: "Person"},
			RawName:          "Apple Inc.",
		}
		resolvedPerson, err := session.ResolveOrInsertEntity(ctx, ePerson)
		require.NoError(t, err)
		assert.Equal(t, resolved.ID, resolvedPerson.ID)
		assert.Equal(t, "Company", resolvedPerson.Type) // Still "Company" as it's the existing entity
	})

	t.Run("GetEntityNeighbours", func(t *testing.T) {
		// Clean slate
		_, _ = sqlDB.ExecContext(ctx, "TRUNCATE TABLE triples, predicates, entities, entity_aliases, ontology_triples, ontology_predicates, ontology_entities, ontology_versions RESTART IDENTITY CASCADE")

		// 1. Seed ontology
		def := Definition{
			Entities: []EntityDefinition{
				{Type: "Person"},
				{Type: "Company"},
			},
			Predicates: []PredicateDefinition{
				{Type: "works_at"},
				{Type: "knows"},
			},
		}
		def.Triples = []TripleDefinition{
			{Subject: &def.Entities[0], Predicate: &def.Predicates[0], Object: &def.Entities[1]}, // Person works_at Company
			{Subject: &def.Entities[0], Predicate: &def.Predicates[1], Object: &def.Entities[0]}, // Person knows Person
		}
		version, err := db.SeedOntology(ctx, sqlDB, def, SeedOptions{Slug: "v-neighbours"})
		require.NoError(t, err)
		session := db.NewSession(version)

		// 2. Setup Entities
		alice, _ := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "Alice"})
		bob, _ := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "Bob"})
		apple, _ := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Company"}, RawName: "Apple"})
		google, _ := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Company"}, RawName: "Google"})

		// 3. Setup Aliases
		// AliceAlias -> Alice
		aliceAlias, _ := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "Alice Alias"})
		_, err = session.LinkEntityAlias(ctx, aliceAlias.ID, alice.ID, nil)
		require.NoError(t, err)

		// 4. Setup Triples
		pWorksAt, err := session.InsertPredicate(ctx, Predicate{Persistable: Persistable{OntologyVersionID: version.ID}, PredicateDefinition: PredicateDefinition{Type: "works_at"}})
		require.NoError(t, err)
		pKnows, err := session.InsertPredicate(ctx, Predicate{Persistable: Persistable{OntologyVersionID: version.ID}, PredicateDefinition: PredicateDefinition{Type: "knows"}})
		require.NoError(t, err)

		// Alice works_at Apple
		t1, err := session.InsertTriple(ctx, Triple{Persistable: Persistable{OntologyVersionID: version.ID}, SubjectEntityID: alice.ID, PredicateID: pWorksAt.ID, ObjectEntityID: apple.ID})
		require.NoError(t, err)
		// Alice Alias works_at Google
		t2, err := session.InsertTriple(ctx, Triple{Persistable: Persistable{OntologyVersionID: version.ID}, SubjectEntityID: aliceAlias.ID, PredicateID: pWorksAt.ID, ObjectEntityID: google.ID})
		require.NoError(t, err)
		// Bob knows Alice
		t3, err := session.InsertTriple(ctx, Triple{Persistable: Persistable{OntologyVersionID: version.ID}, SubjectEntityID: bob.ID, PredicateID: pKnows.ID, ObjectEntityID: alice.ID})
		require.NoError(t, err)

		t.Run("Basic Outgoing and Incoming with Alias Expansion", func(t *testing.T) {
			neighbours, err := session.GetEntityNeighbours(ctx, alice.ID)
			require.NoError(t, err)
			assert.Len(t, neighbours, 3)

			// Expect:
			// 1. Outgoing to Apple (from Alice)
			// 2. Outgoing to Google (from Alice Alias)
			// 3. Incoming from Bob (to Alice)

			foundApple := false
			foundGoogle := false
			foundBob := false

			for _, n := range neighbours {
				if n.EntityID == apple.ID {
					assert.Equal(t, DirectionOutgoing, n.Direction)
					assert.Equal(t, "works_at", n.PredicateType)
					assert.Equal(t, t1.ID, n.TripleID)
					foundApple = true
				} else if n.EntityID == google.ID {
					assert.Equal(t, DirectionOutgoing, n.Direction)
					assert.Equal(t, "works_at", n.PredicateType)
					assert.Equal(t, t2.ID, n.TripleID)
					foundGoogle = true
				} else if n.EntityID == bob.ID {
					assert.Equal(t, DirectionIncoming, n.Direction)
					assert.Equal(t, "knows", n.PredicateType)
					assert.Equal(t, t3.ID, n.TripleID)
					foundBob = true
				}
			}
			assert.True(t, foundApple)
			assert.True(t, foundGoogle)
			assert.True(t, foundBob)

			// Querying through Alias should return same results
			neighbours2, err := session.GetEntityNeighbours(ctx, aliceAlias.ID)
			require.NoError(t, err)
			assert.Len(t, neighbours2, 3)
		})

		t.Run("Neighbour Canonical Normalization", func(t *testing.T) {
			// BobAlias -> Bob
			bobAlias, _ := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "Bob Alias"})
			_, err = session.LinkEntityAlias(ctx, bobAlias.ID, bob.ID, nil)
			require.NoError(t, err)

			// New triple: Alice knows BobAlias
			t4, _ := session.InsertTriple(ctx, Triple{SubjectEntityID: alice.ID, PredicateID: pKnows.ID, ObjectEntityID: bobAlias.ID})

			neighbours, err := session.GetEntityNeighbours(ctx, alice.ID)
			require.NoError(t, err)
			assert.Len(t, neighbours, 4)

			foundBobCount := 0
			for _, n := range neighbours {
				if n.EntityID == bob.ID {
					foundBobCount++
					// Even if stored with bobAlias.ID, it should return bob.ID
					if n.TripleID == t4.ID {
						assert.Equal(t, DirectionOutgoing, n.Direction)
					}
				}
			}
			// One from Bob knows Alice (incoming), one from Alice knows BobAlias (outgoing)
			assert.Equal(t, 2, foundBobCount)
		})

		t.Run("No Deduplication", func(t *testing.T) {
			// Alice works_at Apple (again, distinct triple)
			// Actually Triple has UNIQUE constraint on (version, subject, predicate, object)
			// So we need a different predicate or different version to have "duplicate" facts if they are truly identical.
			// But the requirement says "different triples are different facts".

			// Let's add another 'works_at' predicate with different metadata/validity to allow another triple
			pWorksAt2, err := session.InsertPredicate(ctx, Predicate{
				Persistable:         Persistable{OntologyVersionID: version.ID},
				PredicateDefinition: PredicateDefinition{Type: "works_at", Metadata: json.RawMessage(`{"redundant": true}`)},
			})
			require.NoError(t, err)
			// Note: triple.SubjectEntityID and triple.ObjectEntityID are canonicalized in InsertTriple.
			// To ensure a distinct triple, we MUST have a different object entity here.
			// Google is already used in t2 (Alice Alias -> Google).
			// Let's create a new company for this test.
			microsoft, _ := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Company"}, RawName: "Microsoft"})
			_, err = session.InsertTriple(ctx, Triple{Persistable: Persistable{OntologyVersionID: version.ID}, SubjectEntityID: aliceAlias.ID, PredicateID: pWorksAt2.ID, ObjectEntityID: microsoft.ID})
			require.NoError(t, err)

			neighbours, err := session.GetEntityNeighbours(ctx, alice.ID)
			require.NoError(t, err)

			// Previous: Apple (t1), Google (t2), Bob (t3), Bob (t4)
			// New: Microsoft (t5)
			assert.Len(t, neighbours, 5)

			microsoftTriples := 0
			for _, n := range neighbours {
				if n.EntityID == microsoft.ID {
					microsoftTriples++
				}
			}
			assert.Equal(t, 1, microsoftTriples)
		})

		t.Run("Version Scoping", func(t *testing.T) {
			version2, _ := db.SeedOntology(ctx, sqlDB, def, SeedOptions{Slug: "v-neighbours-2"})
			session2 := db.NewSession(version2)

			// Querying Alice in session2 should fail because Alice was created in session1's version
			_, err := session2.GetEntityNeighbours(ctx, alice.ID)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "not found")
		})

		t.Run("Unknown Entity", func(t *testing.T) {
			_, err := session.GetEntityNeighbours(ctx, "non-existent-id")
			assert.Error(t, err)
		})

		t.Run("No Neighbours", func(t *testing.T) {
			lonely, _ := session.InsertEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "Lonely"})
			neighbours, err := session.GetEntityNeighbours(ctx, lonely.ID)
			require.NoError(t, err)
			assert.Empty(t, neighbours)
		})
	})
}
