package acropora

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testEntity(t *testing.T, ctx context.Context, session *Session, db *DB, sqlDB *sql.DB, version *OntologyVersion) {
	t.Run("EntityDefinition insert and read", func(t *testing.T) {
		e := Entity{
			EntityDefinition: EntityDefinition{
				Type:     "Person",
				Metadata: json.RawMessage(`{"age": 30}`),
			},
			RawName: "  John   Doe  ",
		}
		inserted, err := session.MatchEntity(ctx, e)
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
		_, err := session.MatchEntity(ctx, e)
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
				inserted, err := session.MatchEntity(ctx, e)
				require.NoError(t, err)
				assert.Equal(t, tc.expected, inserted.CanonicalName)
			})
		}
	})

	t.Run("Identity-Aware Match", func(t *testing.T) {
		e1 := Entity{
			EntityDefinition: EntityDefinition{Type: "Person"},
			RawName:          "Unique Name",
		}
		inserted1, err := session.MatchEntity(ctx, e1)
		require.NoError(t, err)

		e2 := Entity{
			EntityDefinition: EntityDefinition{Type: "Person"},
			RawName:          "unique name", // Different raw, same canonical
		}
		inserted2, err := session.MatchEntity(ctx, e2)
		require.NoError(t, err, "Should match existing entity instead of failing")
		assert.Equal(t, inserted1.ID, inserted2.ID, "Should return the same entity ID")

		// Same canonical name in different ontology version should pass
		def2 := Definition{
			Entities: []EntityDefinition{{Type: "Person"}},
		}
		version2, err := db.SeedOntology(ctx, sqlDB, def2, SeedOptions{Slug: "v2-uniqueness"})
		require.NoError(t, err)
		session2 := db.NewSession(version2)

		inserted3, err := session2.MatchEntity(ctx, e1)
		assert.NoError(t, err, "Should allow same canonical name in different ontology version")
		assert.NotEqual(t, inserted1.ID, inserted3.ID, "Should be a different entity ID in different version")
	})

	t.Run("Lookup by raw name", func(t *testing.T) {
		raw := "  Lookup   Me  "
		e := Entity{
			EntityDefinition: EntityDefinition{Type: "Person"},
			RawName:          raw,
		}
		inserted, err := session.MatchEntity(ctx, e)
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
		canonical, err := session.MatchEntity(ctx, Entity{
			EntityDefinition: EntityDefinition{Type: "Company"},
			RawName:          "Canonical Corp",
		})
		require.NoError(t, err)

		alias, err := session.MatchEntity(ctx, Entity{
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

	t.Run("Entity Aliasing", func(t *testing.T) {
		// Setup
		a, err := session.MatchEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "A"})
		require.NoError(t, err)
		b, err := session.MatchEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "B"})
		require.NoError(t, err)
		c, err := session.MatchEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Company"}, RawName: "C"})
		require.NoError(t, err)
		worksAt, err := session.MatchPredicate(ctx, Predicate{PredicateDefinition: PredicateDefinition{Type: "works_at"}})
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
		_, err = session.MatchTriple(ctx, Triple{SubjectEntityID: a.ID, PredicateID: worksAt.ID, ObjectEntityID: c.ID})
		require.NoError(t, err)
		// B already linked to A. Triples on B stay on B.
		// Let's insert one on B directly into DB to simulate historical triple if needed,
		// but the instruction says "old triples on B stay on B physically".
		// To test this we need a triple that WAS on B before it was linked, OR just insert it bypass canonicalization if we want to be sure.
		// Actually, let's just use the Session to insert it BEFORE linking.

		d, err := session.MatchEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Company"}, RawName: "D"})
		require.NoError(t, err)
		b2, err := session.MatchEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "B2"})
		require.NoError(t, err)
		_, err = session.MatchTriple(ctx, Triple{SubjectEntityID: b2.ID, PredicateID: worksAt.ID, ObjectEntityID: d.ID})
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
		e, err := session.MatchEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Company"}, RawName: "E"})
		require.NoError(t, err)
		tr, err := session.MatchTriple(ctx, Triple{SubjectEntityID: b.ID, PredicateID: worksAt.ID, ObjectEntityID: e.ID})
		require.NoError(t, err)
		assert.Equal(t, a.ID, tr.SubjectEntityID)

		// Test D: Child alias reparenting
		// D -> B, then B -> A
		entD, err := session.MatchEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "D_ent"})
		require.NoError(t, err)
		entB, err := session.MatchEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "B_ent"})
		require.NoError(t, err)
		entA, err := session.MatchEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "A_ent"})
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
		def := Definition{
			Entities:   []EntityDefinition{{Type: "Person"}, {Type: "Company"}},
			Predicates: []PredicateDefinition{{Type: "works_at"}},
		}
		def2 := Definition{
			Entities:   append([]EntityDefinition{{Type: "Dummy"}}, def.Entities...),
			Predicates: def.Predicates,
		}
		// Re-point triples to the new entities slice to pass pointer validation in SeedOntology
		def2.Triples = []TripleDefinition{
			{
				Subject:   def2.Entities[1],   // "Person"
				Predicate: def2.Predicates[0], // "works_at"
				Object:    def2.Entities[2],   // "Company"
			},
		}

		version2, err := db.SeedOntology(ctx, sqlDB, def2, SeedOptions{Slug: "v2-aliasing"})
		require.NoError(t, err)
		session2 := db.NewSession(version2)
		entV2, err := session2.MatchEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "V2_Person"})
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

	t.Run("MatchEntity", func(t *testing.T) {
		// Re-seed since previous tests might have truncated or we want a fresh start
		def := Definition{
			Entities: []EntityDefinition{{Type: "Company"}, {Type: "Person"}, {Type: "Other"}},
		}
		version, err := db.SeedOntology(ctx, sqlDB, def, SeedOptions{Slug: "v-match-entity-unique"})
		require.NoError(t, err)
		session := db.NewSession(version)

		// Test A: Insert new entity
		e := Entity{
			EntityDefinition: EntityDefinition{Type: "Company"},
			RawName:          "Apple Inc.",
		}
		resolved, err := session.MatchEntity(ctx, e)
		require.NoError(t, err)
		assert.NotEmpty(t, resolved.ID)
		assert.Equal(t, "Apple Inc.", resolved.RawName)
		assert.Equal(t, "apple inc.", resolved.CanonicalName)

		// Test B: Resolve existing entity by canonical name
		e2 := Entity{
			EntityDefinition: EntityDefinition{Type: "Company"},
			RawName:          "  APPLE  INC.  ",
		}
		resolved2, err := session.MatchEntity(ctx, e2)
		require.NoError(t, err)
		assert.Equal(t, resolved.ID, resolved2.ID)
		assert.Equal(t, "Apple Inc.", resolved2.RawName)

		// Test C: Resolve through alias
		e3 := Entity{
			EntityDefinition: EntityDefinition{Type: "Company"},
			RawName:          "Apple",
		}
		// MatchEntity will first try to find "Apple", not find it, then insert it.
		inserted3, err := session.MatchEntity(ctx, e3)
		require.NoError(t, err)

		// Link "Apple" as an alias of "Apple Inc."
		_, err = session.LinkEntityAlias(ctx, inserted3.ID, resolved.ID, nil)
		require.NoError(t, err)

		// Now MatchEntity "Apple" should return "Apple Inc."
		eResolve := Entity{
			EntityDefinition: EntityDefinition{Type: "Company"},
			RawName:          "Apple",
		}
		resolvedAlias, err := session.MatchEntity(ctx, eResolve)
		require.NoError(t, err)
		assert.Equal(t, resolved.ID, resolvedAlias.ID)
		assert.Equal(t, "Apple Inc.", resolvedAlias.RawName)

		// Test D: Different type with same name
		ePerson := Entity{
			EntityDefinition: EntityDefinition{Type: "Person"},
			RawName:          "Apple Inc.",
		}
		resolvedPerson, err := session.MatchEntity(ctx, ePerson)
		require.NoError(t, err)
		assert.Equal(t, resolved.ID, resolvedPerson.ID)
		assert.Equal(t, "Company", resolvedPerson.Type) // Still "Company" as it's the existing entity

		// Test E: Version scoping
		defV2 := Definition{
			Entities: []EntityDefinition{{Type: "Person"}, {Type: "Company"}}, // Changed to avoid hash collision if previous was same
		}
		version2, err := db.SeedOntology(ctx, sqlDB, defV2, SeedOptions{Slug: "v2-match-scoping"})
		require.NoError(t, err)
		session2 := db.NewSession(version2)

		eMatch := Entity{
			EntityDefinition: EntityDefinition{Type: "Person"},
			RawName:          "Apple Inc.",
		}
		resolvedV2, err := session2.MatchEntity(ctx, eMatch)
		require.NoError(t, err)
		assert.NotEqual(t, resolved.ID, resolvedV2.ID, "Should not match entity from another version")

		// Test F: Ontology validation failure
		eInvalid := Entity{
			EntityDefinition: EntityDefinition{Type: "InvalidType"},
			RawName:          "Valid Name",
		}
		_, err = session.MatchEntity(ctx, eInvalid)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed by ontology")
	})

	t.Run("GetEntityNeighbours", func(t *testing.T) {
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
			{Subject: def.Entities[0], Predicate: def.Predicates[0], Object: def.Entities[1]}, // Person works_at Company
			{Subject: def.Entities[0], Predicate: def.Predicates[1], Object: def.Entities[0]}, // Person knows Person
		}
		version, err := db.SeedOntology(ctx, sqlDB, def, SeedOptions{Slug: "v-neighbours"})
		require.NoError(t, err)
		session := db.NewSession(version)

		// 2. Setup Entities
		alice, _ := session.MatchEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "Alice"})
		bob, _ := session.MatchEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "Bob"})
		apple, _ := session.MatchEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Company"}, RawName: "Apple"})
		google, _ := session.MatchEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Company"}, RawName: "Google"})

		// 3. Setup Aliases
		// AliceAlias -> Alice
		aliceAlias, _ := session.MatchEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "Alice Alias"})
		_, err = session.LinkEntityAlias(ctx, aliceAlias.ID, alice.ID, nil)
		require.NoError(t, err)

		// 4. Setup Triples
		pWorksAt, err := session.MatchPredicate(ctx, Predicate{Persistable: Persistable{OntologyVersionID: version.ID}, PredicateDefinition: PredicateDefinition{Type: "works_at"}})
		require.NoError(t, err)
		pKnows, err := session.MatchPredicate(ctx, Predicate{Persistable: Persistable{OntologyVersionID: version.ID}, PredicateDefinition: PredicateDefinition{Type: "knows"}})
		require.NoError(t, err)

		// Alice works_at Apple
		t1, err := session.MatchTriple(ctx, Triple{Persistable: Persistable{OntologyVersionID: version.ID}, SubjectEntityID: alice.ID, PredicateID: pWorksAt.ID, ObjectEntityID: apple.ID})
		require.NoError(t, err)
		// Alice Alias works_at Google
		t2, err := session.MatchTriple(ctx, Triple{Persistable: Persistable{OntologyVersionID: version.ID}, SubjectEntityID: aliceAlias.ID, PredicateID: pWorksAt.ID, ObjectEntityID: google.ID})
		require.NoError(t, err)
		// Bob knows Alice
		t3, err := session.MatchTriple(ctx, Triple{Persistable: Persistable{OntologyVersionID: version.ID}, SubjectEntityID: bob.ID, PredicateID: pKnows.ID, ObjectEntityID: alice.ID})
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
			bobAlias, _ := session.MatchEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "Bob Alias"})
			_, err = session.LinkEntityAlias(ctx, bobAlias.ID, bob.ID, nil)
			require.NoError(t, err)

			// New triple: Alice knows BobAlias
			t4, _ := session.MatchTriple(ctx, Triple{SubjectEntityID: alice.ID, PredicateID: pKnows.ID, ObjectEntityID: bobAlias.ID})

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
			pWorksAt2, err := session.MatchPredicate(ctx, Predicate{
				Persistable:         Persistable{OntologyVersionID: version.ID},
				PredicateDefinition: PredicateDefinition{Type: "works_at", Metadata: json.RawMessage(`{"redundant": true}`)},
			})
			require.NoError(t, err)
			// Note: triple.SubjectEntityID and triple.ObjectEntityID are canonicalized in MatchTriple.
			// To ensure a distinct triple, we MUST have a different object entity here.
			// Google is already used in t2 (Alice Alias -> Google).
			// Let's create a new company for this test.
			microsoft, _ := session.MatchEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Company"}, RawName: "Microsoft"})
			_, err = session.MatchTriple(ctx, Triple{Persistable: Persistable{OntologyVersionID: version.ID}, SubjectEntityID: aliceAlias.ID, PredicateID: pWorksAt2.ID, ObjectEntityID: microsoft.ID})
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
			lonely, _ := session.MatchEntity(ctx, Entity{EntityDefinition: EntityDefinition{Type: "Person"}, RawName: "Lonely"})
			neighbours, err := session.GetEntityNeighbours(ctx, lonely.ID)
			require.NoError(t, err)
			assert.Empty(t, neighbours)
		})
	})
}
