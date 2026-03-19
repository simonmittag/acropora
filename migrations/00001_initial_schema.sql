-- +goose Up
-- +goose StatementBegin
CREATE TABLE ontology_versions (
    id BIGSERIAL PRIMARY KEY,
    version_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE ontology_entities (
    id BIGSERIAL PRIMARY KEY,
    ontology_version_id BIGINT NOT NULL REFERENCES ontology_versions(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    UNIQUE (ontology_version_id, name)
);

CREATE TABLE ontology_predicates (
    id BIGSERIAL PRIMARY KEY,
    ontology_version_id BIGINT NOT NULL REFERENCES ontology_versions(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    UNIQUE (ontology_version_id, name)
);

CREATE TABLE ontology_triples (
    id BIGSERIAL PRIMARY KEY,
    ontology_version_id BIGINT NOT NULL REFERENCES ontology_versions(id) ON DELETE CASCADE,
    subject_entity_name TEXT NOT NULL,
    predicate_name TEXT NOT NULL,
    object_entity_name TEXT NOT NULL,
    UNIQUE (ontology_version_id, subject_entity_name, predicate_name, object_entity_name)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE ontology_triples;
DROP TABLE ontology_predicates;
DROP TABLE ontology_entities;
DROP TABLE ontology_versions;
-- +goose StatementEnd
