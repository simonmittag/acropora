package db

import (
	"context"
	"database/sql"
	"fmt"
)

func GetPrefix(ctx context.Context) string {
	if p, ok := ctx.Value("table_prefix").(string); ok {
		return p
	}
	return "acropora"
}

func Migrate(ctx context.Context, db *sql.DB, prefix string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	versionTable := TableName(prefix, TableDbVersion)

	// Check if version table exists
	var exists bool
	query := `SELECT EXISTS (
		SELECT FROM information_schema.tables 
		WHERE table_name = $1
	)`
	err = tx.QueryRowContext(ctx, query, versionTable).Scan(&exists)
	if err != nil {
		return err
	}

	if !exists {
		if err := migration_000001(ctx, tx, prefix); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func migration_000001(ctx context.Context, tx *sql.Tx, p string) error {
	_, err := tx.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			version INTEGER PRIMARY KEY,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		INSERT INTO %s (version) VALUES (1);

		CREATE TABLE %s (
			id TEXT PRIMARY KEY,
			hash TEXT NOT NULL UNIQUE,
			slug TEXT NOT NULL UNIQUE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		CREATE TABLE %s (
			id TEXT PRIMARY KEY,
			ontology_version_id TEXT NOT NULL REFERENCES %s(id) ON DELETE CASCADE,
			type TEXT NOT NULL,
			metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		CREATE INDEX idx_ontology_entities_ontology_version_id ON %s(ontology_version_id);
		CREATE INDEX idx_ontology_entities_ontology_version_id_type ON %s(ontology_version_id, type);

		CREATE TABLE %s (
			id TEXT PRIMARY KEY,
			ontology_version_id TEXT NOT NULL REFERENCES %s(id) ON DELETE CASCADE,
			type TEXT NOT NULL,
			metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		CREATE INDEX idx_ontology_predicates_ontology_version_id ON %s(ontology_version_id);
		CREATE INDEX idx_ontology_predicates_ontology_version_id_type ON %s(ontology_version_id, type);

		CREATE TABLE %s (
			id TEXT PRIMARY KEY,
			ontology_version_id TEXT NOT NULL REFERENCES %s(id) ON DELETE CASCADE,
			subject_entity_id TEXT NOT NULL REFERENCES %s(id) ON DELETE CASCADE,
			predicate_id TEXT NOT NULL REFERENCES %s(id) ON DELETE CASCADE,
			object_entity_id TEXT NOT NULL REFERENCES %s(id) ON DELETE CASCADE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (ontology_version_id, subject_entity_id, predicate_id, object_entity_id)
		);

		CREATE INDEX idx_ontology_triples_ontology_version_id ON %s(ontology_version_id);
		CREATE INDEX idx_ontology_triples_subject_entity_id ON %s(subject_entity_id);
		CREATE INDEX idx_ontology_triples_object_entity_id ON %s(object_entity_id);
		CREATE INDEX idx_ontology_triples_predicate_id ON %s(predicate_id);
		CREATE INDEX idx_ontology_triples_all_fields ON %s(ontology_version_id, subject_entity_id, predicate_id, object_entity_id);

		CREATE TABLE %s (
			id TEXT PRIMARY KEY,
			ontology_version_id TEXT NOT NULL REFERENCES %s(id),
			type TEXT NOT NULL,
			raw_name TEXT NOT NULL,
			canonical_name TEXT NOT NULL,
			metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (ontology_version_id, canonical_name)
		);

		CREATE INDEX idx_entities_ontology_version_id ON %s(ontology_version_id);
		CREATE INDEX idx_entities_ontology_version_id_id ON %s(ontology_version_id, id);

		CREATE TABLE %s (
			id TEXT PRIMARY KEY,
			ontology_version_id TEXT NOT NULL REFERENCES %s(id),
			type TEXT NOT NULL,
			metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
			valid_from TIMESTAMPTZ NULL,
			valid_to TIMESTAMPTZ NULL,
			dedup_hash TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (ontology_version_id, dedup_hash)
		);

		CREATE INDEX idx_predicates_ontology_version_id ON %s(ontology_version_id);
		CREATE INDEX idx_predicates_type ON %s(type);
		CREATE INDEX idx_predicates_ontology_version_id_type ON %s(ontology_version_id, type);
		CREATE INDEX idx_predicates_dedup_hash ON %s(dedup_hash);

		CREATE TABLE %s (
			id TEXT PRIMARY KEY,
			ontology_version_id TEXT NOT NULL REFERENCES %s(id),
			subject_entity_id TEXT NOT NULL REFERENCES %s(id),
			predicate_id TEXT NOT NULL REFERENCES %s(id),
			object_entity_id TEXT NOT NULL REFERENCES %s(id),
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (ontology_version_id, subject_entity_id, predicate_id, object_entity_id)
		);

		CREATE INDEX idx_triples_subject_entity_id ON %s(subject_entity_id);
		CREATE INDEX idx_triples_object_entity_id ON %s(object_entity_id);
		CREATE INDEX idx_triples_predicate_id ON %s(predicate_id);
		CREATE INDEX idx_triples_all_fields ON %s(ontology_version_id, subject_entity_id, predicate_id, object_entity_id);

		CREATE TABLE %s (
			id TEXT PRIMARY KEY,
			ontology_version_id TEXT NOT NULL REFERENCES %s(id),
			alias_entity_id TEXT NOT NULL REFERENCES %s(id),
			canonical_entity_id TEXT NOT NULL REFERENCES %s(id),
			metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (ontology_version_id, alias_entity_id)
		);

		CREATE INDEX idx_entity_aliases_ontology_version_id ON %s(ontology_version_id);
		CREATE INDEX idx_entity_aliases_alias_entity_id ON %s(alias_entity_id);
		CREATE INDEX idx_entity_aliases_canonical_entity_id ON %s(canonical_entity_id);
	`,
		TableName(p, TableDbVersion),
		TableName(p, TableDbVersion),
		TableName(p, TableOntologyVersions),
		TableName(p, TableOntologyEntities), TableName(p, TableOntologyVersions),
		TableName(p, TableOntologyEntities), TableName(p, TableOntologyEntities),
		TableName(p, TableOntologyPredicates), TableName(p, TableOntologyVersions),
		TableName(p, TableOntologyPredicates), TableName(p, TableOntologyPredicates),
		TableName(p, TableOntologyTriples), TableName(p, TableOntologyVersions), TableName(p, TableOntologyEntities), TableName(p, TableOntologyPredicates), TableName(p, TableOntologyEntities),
		TableName(p, TableOntologyTriples), TableName(p, TableOntologyTriples), TableName(p, TableOntologyTriples), TableName(p, TableOntologyTriples), TableName(p, TableOntologyTriples),
		TableName(p, TableEntities), TableName(p, TableOntologyVersions),
		TableName(p, TableEntities), TableName(p, TableEntities),
		TableName(p, TablePredicates), TableName(p, TableOntologyVersions),
		TableName(p, TablePredicates), TableName(p, TablePredicates), TableName(p, TablePredicates), TableName(p, TablePredicates),
		TableName(p, TableTriples), TableName(p, TableOntologyVersions), TableName(p, TableEntities), TableName(p, TablePredicates), TableName(p, TableEntities),
		TableName(p, TableTriples), TableName(p, TableTriples), TableName(p, TableTriples), TableName(p, TableTriples),
		TableName(p, TableEntityAliases), TableName(p, TableOntologyVersions), TableName(p, TableEntities), TableName(p, TableEntities),
		TableName(p, TableEntityAliases), TableName(p, TableEntityAliases), TableName(p, TableEntityAliases),
	))
	return err
}
