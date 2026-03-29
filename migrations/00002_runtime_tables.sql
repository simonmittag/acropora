-- +goose Up
-- +goose StatementBegin
CREATE TABLE entities (
    id TEXT PRIMARY KEY,
    ontology_version_id TEXT NOT NULL REFERENCES ontology_versions(id),
    type TEXT NOT NULL,
    raw_name TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ontology_version_id, canonical_name)
);

CREATE INDEX idx_entities_ontology_version_id ON entities(ontology_version_id);
CREATE INDEX idx_entities_ontology_version_id_id ON entities(ontology_version_id, id);

CREATE TABLE predicates (
    id TEXT PRIMARY KEY,
    ontology_version_id TEXT NOT NULL REFERENCES ontology_versions(id),
    type TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    valid_from TIMESTAMPTZ NULL,
    valid_to TIMESTAMPTZ NULL,
    dedup_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ontology_version_id, dedup_hash)
);

CREATE INDEX idx_predicates_ontology_version_id ON predicates(ontology_version_id);
CREATE INDEX idx_predicates_type ON predicates(type);
CREATE INDEX idx_predicates_ontology_version_id_type ON predicates(ontology_version_id, type);
CREATE INDEX idx_predicates_dedup_hash ON predicates(dedup_hash);

CREATE TABLE triples (
    id TEXT PRIMARY KEY,
    ontology_version_id TEXT NOT NULL REFERENCES ontology_versions(id),
    subject_entity_id TEXT NOT NULL REFERENCES entities(id),
    predicate_id TEXT NOT NULL REFERENCES predicates(id),
    object_entity_id TEXT NOT NULL REFERENCES entities(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ontology_version_id, subject_entity_id, predicate_id, object_entity_id)
);

CREATE INDEX idx_triples_subject_entity_id ON triples(subject_entity_id);
CREATE INDEX idx_triples_object_entity_id ON triples(object_entity_id);
CREATE INDEX idx_triples_predicate_id ON triples(predicate_id);
CREATE INDEX idx_triples_all_fields ON triples(ontology_version_id, subject_entity_id, predicate_id, object_entity_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE triples;
DROP TABLE predicates;
DROP TABLE entities;
-- +goose StatementEnd
