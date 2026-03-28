-- +goose Up
-- +goose StatementBegin
CREATE TABLE entity_aliases (
    id TEXT PRIMARY KEY,
    ontology_version_id TEXT NOT NULL REFERENCES ontology_versions(id),
    alias_entity_id TEXT NOT NULL REFERENCES entities(id),
    canonical_entity_id TEXT NOT NULL REFERENCES entities(id),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ontology_version_id, alias_entity_id)
);

CREATE INDEX idx_entity_aliases_ontology_version_id ON entity_aliases(ontology_version_id);
CREATE INDEX idx_entity_aliases_alias_entity_id ON entity_aliases(alias_entity_id);
CREATE INDEX idx_entity_aliases_canonical_entity_id ON entity_aliases(canonical_entity_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE entity_aliases;
-- +goose StatementEnd
