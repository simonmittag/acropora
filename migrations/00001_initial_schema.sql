-- +goose Up
-- +goose StatementBegin
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

CREATE TABLE triples (
    id UUID PRIMARY KEY,
    ontology_version_id UUID NOT NULL REFERENCES ontology_versions(id) ON DELETE CASCADE,
    subject_entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    predicate_id UUID NOT NULL REFERENCES predicates(id) ON DELETE CASCADE,
    object_entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ontology_version_id, subject_entity_id, predicate_id, object_entity_id)
);

CREATE INDEX idx_triples_subject_entity_id ON triples(subject_entity_id);
CREATE INDEX idx_triples_object_entity_id ON triples(object_entity_id);
CREATE INDEX idx_triples_predicate_id ON triples(predicate_id);
CREATE INDEX idx_triples_subject_predicate ON triples(subject_entity_id, predicate_id);
CREATE INDEX idx_triples_object_predicate ON triples(object_entity_id, predicate_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE triples;
DROP TABLE predicates;
DROP TABLE entities;
DROP TABLE ontology_versions;
-- +goose StatementEnd
