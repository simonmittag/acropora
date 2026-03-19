# Acropora

Acropora is an alpha-stage Go library for storing directional subject–predicate–object triples on PostgreSQL.

Its goal is to provide a small, explicit persistence layer for semantically constrained graph-like data without 
introducing a graph database or heavy abstraction. 

Simple mission: be the best friend of teams that have more than one DB table with a JSONB column each.

## Design goals

* Runs on top of any PostgreSQL compatible database as a thin software layer in go.
* Represent facts as triples and version ontology explicitly.
* Validate data before persistence, enforcing referential integrity.
* Stay small enough to embed inside existing systems

## Status

Currently a hack. Work is focused on ontology definition, versioning, and database seeding. Runtime data access 
and query APIs are not complete yet.
