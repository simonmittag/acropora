package db

import (
	"fmt"
)

const (
	TableOntologyVersions   = "ontology_versions"
	TableOntologyEntities   = "ontology_entities"
	TableOntologyPredicates = "ontology_predicates"
	TableOntologyTriples    = "ontology_triples"
	TableEntities           = "entities"
	TablePredicates         = "predicates"
	TableTriples            = "triples"
	TableEntityAliases      = "entity_aliases"
	TableDbVersion          = "db_version"
)

func TableName(prefix, base string) string {
	return fmt.Sprintf("%s_%s", prefix, base)
}
