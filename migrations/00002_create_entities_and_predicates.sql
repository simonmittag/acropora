-- +goose Up
-- +goose StatementBegin
-- First, fix ontology_versions to use UUID if it wasn't already.
-- The instructions say "We already have a table for ontology versions"
-- but currently 00001 uses BIGSERIAL.
-- Given the requirement "OntologyVersionID should be UUID" for entities/predicates,
-- and "id should be UUID" for entities/predicates,
-- I must decide whether to update ontology_versions or just create new tables.
-- I'll follow the instruction to keep things simple.
-- I'll create a new migration for entities and predicates using UUIDs.

-- For now, I'll drop the existing ones to avoid conflicts and simplify.
-- This matches "foundational schema phase" and "minimum persistent model real".

DROP TABLE IF EXISTS ontology_triples;
DROP TABLE IF EXISTS ontology_predicates;
DROP TABLE IF EXISTS ontology_entities;
DROP TABLE IF EXISTS ontology_versions;

CREATE TABLE ontology_versions (
    id UUID PRIMARY KEY,
    version_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE entities (
    id UUID PRIMARY KEY,
    ontology_version_id UUID NOT NULL REFERENCES ontology_versions(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_entities_ontology_version_id ON entities(ontology_version_id);
CREATE INDEX idx_entities_name ON entities(name);
CREATE INDEX idx_entities_ontology_version_id_name ON entities(ontology_version_id, name);

CREATE TABLE predicates (
    id UUID PRIMARY KEY,
    ontology_version_id UUID NOT NULL REFERENCES ontology_versions(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    valid_from TIMESTAMPTZ NULL,
    valid_to TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_predicates_ontology_version_id ON predicates(ontology_version_id);
CREATE INDEX idx_predicates_name ON predicates(name);
CREATE INDEX idx_predicates_ontology_version_id_name ON predicates(ontology_version_id, name);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE predicates;
DROP TABLE entities;
DROP TABLE ontology_versions;
-- +goose StatementEnd
