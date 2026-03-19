-- +goose Up
-- +goose StatementBegin
CREATE TABLE ontology_versions (
    id UUID PRIMARY KEY,
    hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ontology_entities (
    id UUID PRIMARY KEY,
    ontology_version_id UUID NOT NULL REFERENCES ontology_versions(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_ontology_entities_ontology_version_id ON ontology_entities(ontology_version_id);
CREATE INDEX idx_ontology_entities_ontology_version_id_name ON ontology_entities(ontology_version_id, name);

CREATE TABLE ontology_predicates (
    id UUID PRIMARY KEY,
    ontology_version_id UUID NOT NULL REFERENCES ontology_versions(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    valid_from TIMESTAMPTZ NULL,
    valid_to TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_ontology_predicates_ontology_version_id ON ontology_predicates(ontology_version_id);
CREATE INDEX idx_ontology_predicates_ontology_version_id_name ON ontology_predicates(ontology_version_id, name);

CREATE TABLE ontology_triples (
    id UUID PRIMARY KEY,
    ontology_version_id UUID NOT NULL REFERENCES ontology_versions(id) ON DELETE CASCADE,
    subject_entity_id UUID NOT NULL REFERENCES ontology_entities(id) ON DELETE CASCADE,
    predicate_id UUID NOT NULL REFERENCES ontology_predicates(id) ON DELETE CASCADE,
    object_entity_id UUID NOT NULL REFERENCES ontology_entities(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ontology_version_id, subject_entity_id, predicate_id, object_entity_id)
);

CREATE INDEX idx_ontology_triples_ontology_version_id ON ontology_triples(ontology_version_id);
CREATE INDEX idx_ontology_triples_subject_entity_id ON ontology_triples(subject_entity_id);
CREATE INDEX idx_ontology_triples_object_entity_id ON ontology_triples(object_entity_id);
CREATE INDEX idx_ontology_triples_predicate_id ON ontology_triples(predicate_id);
CREATE INDEX idx_ontology_triples_all_fields ON ontology_triples(ontology_version_id, subject_entity_id, predicate_id, object_entity_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE ontology_triples;
DROP TABLE ontology_predicates;
DROP TABLE ontology_entities;
DROP TABLE ontology_versions;
-- +goose StatementEnd
