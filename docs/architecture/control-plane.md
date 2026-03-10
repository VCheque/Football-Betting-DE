# Control Plane Design

## Purpose

The PostgreSQL control plane stores metadata, dimensions, configuration, and data quality results for the platform.

It does not store the raw dataset as the main system of record.

## Why PostgreSQL Exists in This Project

PostgreSQL is used for structured platform data that must be queryable and operationally reliable.

This includes:

- source configuration
- ingestion run history
- file manifests
- league dimensions
- team dimensions
- alias mappings
- data quality results

## MVP Tables

### `source_config`

Stores source-level setup for each pipeline.

Use in MVP:

- register `football_data_co_uk`
- register `matches_odds`
- store source configuration in `config_json`

### `dim_league`

Stores canonical league information.

Use in MVP:

- map source league codes to consistent league names
- support future joins across multiple sources

### `dim_team`

Stores canonical team information.

Use in MVP:

- normalize team identity
- support future joins across multiple sources

### `team_alias_map`

Stores source-specific team aliases.

Use in MVP:

- map raw team names from source files to canonical team identities

### `pipeline_run`

Stores one row per ingestion run.

Use in MVP:

- track status of each raw load
- record start and completion times
- record row counts and checksums

### `file_manifest`

Stores one row per ingested file.

Use in MVP:

- record which file was loaded
- record where it was stored in Bronze
- record checksum, size, and row count

### `dq_results`

Stores data quality outcomes.

Use in MVP:

- store ingestion or transformation quality checks
- allow quality reporting outside raw logs

## Data Ownership Boundary

### MinIO owns

- raw files
- transformed files

### PostgreSQL owns

- metadata
- configuration
- dimensions
- quality results

This separation is important because it prevents the project from turning PostgreSQL into both a control plane and a raw lake.

## Interview Summary

This schema shows:

- operational thinking
- metadata-driven ingestion
- clear separation between dataset storage and platform state
- a foundation for observability and lineage
