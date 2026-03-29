package acropora

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// InsertEntity inserts a new entity into the runtime.
func (s *Session) InsertEntity(ctx context.Context, entity Entity) (Entity, error) {
	if entity.Type == "" {
		return Entity{}, errors.New("entity type cannot be empty")
	}

	trimmedRaw := strings.TrimSpace(entity.RawName)
	if trimmedRaw == "" {
		return Entity{}, errors.New("entity raw name cannot be empty")
	}

	// Compute CanonicalName internally
	entity.CanonicalName = normalizeCanonicalName(entity.RawName)

	// Validate against ontology
	var exists bool
	err := s.db.sqlDB.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM ontology_entities WHERE ontology_version_id = $1 AND type = $2)",
		s.version.ID, entity.Type).Scan(&exists)
	if err != nil {
		return Entity{}, fmt.Errorf("validating entity against ontology: %w", err)
	}
	if !exists {
		return Entity{}, fmt.Errorf("entity type %q not allowed by ontology version %s", entity.Type, s.version.Slug)
	}

	if entity.ID == "" {
		entity.ID = uuid.New().String()
	}
	entity.OntologyVersionID = s.version.ID
	if entity.Metadata == nil {
		entity.Metadata = json.RawMessage("{}")
	}

	now := time.Now()
	err = s.db.sqlDB.QueryRowContext(ctx,
		`INSERT INTO entities (id, ontology_version_id, type, raw_name, canonical_name, metadata, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 RETURNING created_at, updated_at`,
		entity.ID, entity.OntologyVersionID, entity.Type, entity.RawName, entity.CanonicalName, entity.Metadata, now, now).Scan(&entity.CreatedAt, &entity.UpdatedAt)
	if err != nil {
		return Entity{}, fmt.Errorf("inserting entity: %w", err)
	}

	return entity, nil
}

// ResolveOrInsertEntity attempts to find an existing entity by normalizing the name and checking both
// canonical names and known aliases. If not found, it creates a new entity.
func (s *Session) ResolveOrInsertEntity(ctx context.Context, entity Entity) (Entity, error) {
	if entity.Type == "" {
		return Entity{}, errors.New("entity type cannot be empty")
	}

	trimmedRaw := strings.TrimSpace(entity.RawName)
	if trimmedRaw == "" {
		return Entity{}, errors.New("entity raw name cannot be empty")
	}

	// 1. Try to find existing entity by name (handles canonical name and aliases)
	found, err := s.GetEntityByRawName(ctx, entity.RawName)
	if err == nil {
		// Found existing entity. Ensure types match if requested.
		// If the existing entity has a different type, we could either return an error,
		// or just return the found entity. The requirements say "takes a name and an optional entity type".
		// In our Entity struct, Type is required for InsertEntity.
		return found, nil
	}

	// If the error is anything other than "not found", return it.
	if !strings.Contains(err.Error(), "not found") {
		return Entity{}, fmt.Errorf("resolving entity: %w", err)
	}

	// 2. Not found, so insert new entity
	return s.InsertEntity(ctx, entity)
}

// GetEntityByID fetches an entity by its ID, scoped to the session's ontology version.
func (s *Session) GetEntityByID(ctx context.Context, id string) (Entity, error) {
	var e Entity
	err := s.db.sqlDB.QueryRowContext(ctx,
		"SELECT id, ontology_version_id, type, raw_name, canonical_name, metadata, created_at, updated_at FROM entities WHERE id = $1 AND ontology_version_id = $2",
		id, s.version.ID).Scan(&e.ID, &e.OntologyVersionID, &e.Type, &e.RawName, &e.CanonicalName, &e.Metadata, &e.CreatedAt, &e.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Entity{}, fmt.Errorf("entity %s not found in session ontology version", id)
		}
		return Entity{}, fmt.Errorf("fetching entity: %w", err)
	}
	return e, nil
}

// GetEntityByRawName fetches an entity by its raw name, applying conservative canonicalization and anti-alias resolution.
func (s *Session) GetEntityByRawName(ctx context.Context, rawName string) (Entity, error) {
	if strings.TrimSpace(rawName) == "" {
		return Entity{}, errors.New("raw name cannot be empty")
	}

	canonicalName := normalizeCanonicalName(rawName)

	var e Entity
	err := s.db.sqlDB.QueryRowContext(ctx,
		"SELECT id, ontology_version_id, type, raw_name, canonical_name, metadata, created_at, updated_at FROM entities WHERE canonical_name = $1 AND ontology_version_id = $2",
		canonicalName, s.version.ID).Scan(&e.ID, &e.OntologyVersionID, &e.Type, &e.RawName, &e.CanonicalName, &e.Metadata, &e.CreatedAt, &e.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Entity{}, fmt.Errorf("entity %q not found", rawName)
		}
		return Entity{}, fmt.Errorf("fetching entity by raw name: %w", err)
	}

	// Internal anti-alias resolution
	canonicalID, err := s.GetAntiAliasedEntityID(ctx, e.ID)
	if err != nil {
		return Entity{}, fmt.Errorf("resolving entity alias: %w", err)
	}

	if canonicalID != e.ID {
		return s.GetEntityByID(ctx, canonicalID)
	}

	return e, nil
}

// LinkEntityAlias links an alias entity to a canonical root entity.
func (s *Session) LinkEntityAlias(ctx context.Context, aliasEntityID, canonicalEntityID string, metadata json.RawMessage) (EntityAlias, error) {
	if aliasEntityID == "" || canonicalEntityID == "" {
		return EntityAlias{}, errors.New("entity IDs cannot be empty")
	}
	if aliasEntityID == canonicalEntityID {
		return EntityAlias{}, errors.New("cannot link entity to itself")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return EntityAlias{}, err
	}
	defer tx.Rollback()

	// 1. Ensure both exist and belong to session ontology version
	var aType, cType string
	err = tx.QueryRowContext(ctx, "SELECT type FROM entities WHERE id = $1 AND ontology_version_id = $2", aliasEntityID, s.version.ID).Scan(&aType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return EntityAlias{}, fmt.Errorf("alias entity %s not found in session ontology version", aliasEntityID)
		}
		return EntityAlias{}, fmt.Errorf("checking alias entity %s: %w", aliasEntityID, err)
	}
	err = tx.QueryRowContext(ctx, "SELECT type FROM entities WHERE id = $1 AND ontology_version_id = $2", canonicalEntityID, s.version.ID).Scan(&cType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return EntityAlias{}, fmt.Errorf("canonical entity %s not found in session ontology version", canonicalEntityID)
		}
		return EntityAlias{}, fmt.Errorf("checking canonical entity %s: %w", canonicalEntityID, err)
	}

	// 2. Resolve canonicalEntityID to its canonical root
	rootID, err := s.getCanonicalEntityIDTx(ctx, tx, canonicalEntityID)
	if err != nil {
		return EntityAlias{}, err
	}

	// 3. Resolve aliasEntityID to its root (to check for cycles or redundant moves)
	aliasRootID, err := s.getCanonicalEntityIDTx(ctx, tx, aliasEntityID)
	if err != nil {
		return EntityAlias{}, err
	}

	if aliasRootID == rootID {
		return EntityAlias{}, errors.New("entities are already in the same alias group")
	}

	// 4. Reject if operation would create cycle (if rootID is an alias of aliasEntityID - impossible if graph is flat and we resolved rootID)
	// But let's be explicit: if someone tries to link A -> B when B -> A exists.
	// getCanonicalEntityIDTx(B) would return A. So we'd try to link A -> A, which is rejected above.

	// 5. Reparent any aliases currently pointing to aliasEntityID (or its group if it was a root)
	// Actually, if aliasEntityID was a root, we move it and ALL its children to the new root.
	// If aliasEntityID was already an alias, we move JUST it (it has no children).
	// But according to rule 4: "migrate C's alias link to point directly to A".
	// This means if B -> A, and we link A -> X, then B must point to X.
	_, err = tx.ExecContext(ctx,
		"UPDATE entity_aliases SET canonical_entity_id = $1, updated_at = now() WHERE canonical_entity_id = $2 AND ontology_version_id = $3",
		rootID, aliasEntityID, s.version.ID)
	if err != nil {
		return EntityAlias{}, fmt.Errorf("reparenting aliases: %w", err)
	}

	// 6. Create or update alias link for aliasEntityID -> rootID
	if metadata == nil {
		metadata = json.RawMessage("{}")
	}
	now := time.Now()
	var ea EntityAlias
	ea.ID = uuid.New().String()
	ea.AliasEntityID = aliasEntityID
	ea.CanonicalEntityID = rootID
	ea.Metadata = metadata
	ea.OntologyVersionID = s.version.ID

	err = tx.QueryRowContext(ctx,
		`INSERT INTO entity_aliases (id, ontology_version_id, alias_entity_id, canonical_entity_id, metadata, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (ontology_version_id, alias_entity_id) DO UPDATE 
		 SET canonical_entity_id = EXCLUDED.canonical_entity_id, 
		     metadata = EXCLUDED.metadata,
		     updated_at = EXCLUDED.updated_at
		 RETURNING id, created_at, updated_at`,
		ea.ID, ea.OntologyVersionID, ea.AliasEntityID, ea.CanonicalEntityID, ea.Metadata, now, now).Scan(&ea.ID, &ea.CreatedAt, &ea.UpdatedAt)
	if err != nil {
		return EntityAlias{}, fmt.Errorf("upserting alias: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return EntityAlias{}, err
	}

	return ea, nil
}

// GetAntiAliasedEntityID returns the canonical root ID for an entity.
func (s *Session) GetAntiAliasedEntityID(ctx context.Context, entityID string) (string, error) {
	var canonicalID string
	err := s.db.sqlDB.QueryRowContext(ctx,
		"SELECT canonical_entity_id FROM entity_aliases WHERE alias_entity_id = $1 AND ontology_version_id = $2",
		entityID, s.version.ID).Scan(&canonicalID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return entityID, nil
		}
		return "", err
	}
	return canonicalID, nil
}

func (s *Session) getCanonicalEntityIDTx(ctx context.Context, tx *sql.Tx, entityID string) (string, error) {
	var canonicalID string
	err := tx.QueryRowContext(ctx,
		"SELECT canonical_entity_id FROM entity_aliases WHERE alias_entity_id = $1 AND ontology_version_id = $2",
		entityID, s.version.ID).Scan(&canonicalID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return entityID, nil
		}
		return "", err
	}
	return canonicalID, nil
}

// GetAliasGroupEntityIDs returns all entity IDs in the alias group.
func (s *Session) GetAliasGroupEntityIDs(ctx context.Context, entityID string) ([]string, string, error) {
	canonicalID, err := s.GetAntiAliasedEntityID(ctx, entityID)
	if err != nil {
		return nil, "", err
	}

	rows, err := s.db.sqlDB.QueryContext(ctx,
		"SELECT alias_entity_id FROM entity_aliases WHERE canonical_entity_id = $1 AND ontology_version_id = $2",
		canonicalID, s.version.ID)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	group := []string{canonicalID}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, "", err
		}
		group = append(group, id)
	}
	return group, canonicalID, nil
}

// GetEntityNeighbours returns all one-hop neighbours of an entity, expanding across the alias group.
func (s *Session) GetEntityNeighbours(ctx context.Context, entityID string) ([]Neighbour, error) {
	// 1. Resolve input entity to canonical root
	canonicalID, err := s.GetAntiAliasedEntityID(ctx, entityID)
	if err != nil {
		return nil, fmt.Errorf("resolving canonical entity ID: %w", err)
	}

	// Verify entity exists in this version
	_, err = s.GetEntityByID(ctx, canonicalID)
	if err != nil {
		return nil, fmt.Errorf("fetching canonical entity: %w", err)
	}

	// 2. Expand alias group
	groupIDs, _, err := s.GetAliasGroupEntityIDs(ctx, canonicalID)
	if err != nil {
		return nil, fmt.Errorf("expanding alias group: %w", err)
	}

	// 3. Query all runtime triples where subject or object is in the alias group
	query := `
		SELECT 
			t.id AS triple_id,
			t.subject_entity_id,
			t.object_entity_id,
			p.type AS predicate_type,
			p.metadata AS predicate_metadata,
			p.valid_from AS predicate_valid_from,
			p.valid_to AS predicate_valid_to,
			e.id AS neighbour_entity_id,
			e.type AS neighbour_entity_type,
			e.canonical_name AS neighbour_canonical_name,
			e.metadata AS neighbour_metadata
		FROM triples t
		JOIN predicates p ON t.predicate_id = p.id
		JOIN entities e ON (
			(t.subject_entity_id = ANY($1) AND t.object_entity_id = e.id) OR
			(t.object_entity_id = ANY($1) AND t.subject_entity_id = e.id)
		)
		WHERE t.ontology_version_id = $2
	`

	rows, err := s.db.sqlDB.QueryContext(ctx, query, pq.Array(groupIDs), s.version.ID)
	if err != nil {
		return nil, fmt.Errorf("querying neighbours: %w", err)
	}
	defer rows.Close()

	var neighbours []Neighbour
	groupMap := make(map[string]bool)
	for _, id := range groupIDs {
		groupMap[id] = true
	}

	for rows.Next() {
		var (
			tripleID, subjectID, objectID string
			n                             Neighbour
		)
		err := rows.Scan(
			&tripleID,
			&subjectID,
			&objectID,
			&n.PredicateType,
			&n.PredicateMetadata,
			&n.PredicateValidFrom,
			&n.PredicateValidTo,
			&n.EntityID,
			&n.EntityType,
			&n.CanonicalName,
			&n.Metadata,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning neighbour row: %w", err)
		}

		n.TripleID = tripleID

		// Determine direction relative to queried alias group
		isSubjectInGroup := groupMap[subjectID]
		isObjectInGroup := groupMap[objectID]

		if isSubjectInGroup && isObjectInGroup {
			// Malformed or self-loop: if both are in group, we treat it as outgoing by convention
			n.Direction = DirectionOutgoing
		} else if isSubjectInGroup {
			n.Direction = DirectionOutgoing
		} else {
			n.Direction = DirectionIncoming
		}

		// Normalize returned neighbour entity to canonical identity
		canonicalNeighbourID, err := s.GetAntiAliasedEntityID(ctx, n.EntityID)
		if err != nil {
			return nil, fmt.Errorf("resolving canonical neighbour ID: %w", err)
		}

		if canonicalNeighbourID != n.EntityID {
			// If it's different, we need to fetch the canonical entity's full details
			canonicalNeighbour, err := s.GetEntityByID(ctx, canonicalNeighbourID)
			if err != nil {
				return nil, fmt.Errorf("fetching canonical neighbour entity: %w", err)
			}
			n.EntityID = canonicalNeighbour.ID
			n.EntityType = canonicalNeighbour.Type
			n.CanonicalName = canonicalNeighbour.CanonicalName
			n.Metadata = canonicalNeighbour.Metadata
		}

		neighbours = append(neighbours, n)
	}

	return neighbours, nil
}
