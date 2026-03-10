# Semantic Layer

## Purpose

The semantic layer makes raw and metadata datasets easier to consume downstream.

Instead of forcing dbt or Streamlit to point directly at storage paths and operational tables, Dremio provides stable dataset names.

## Current State

The local Dremio instance is initialized.

Local admin user:

- username: `admin`
- password: `admin12345`

This is a local development account for the MVP.

## Why Dremio Exists Here

Dremio sits between:

- MinIO raw and curated storage
- PostgreSQL metadata and dimensions
- dbt transformations
- Streamlit consumption

This keeps downstream tools from needing to understand storage-level details.

## What Should Be Exposed in Dremio First

For the MVP, create these stable datasets:

### Source: MinIO

Create a source for the MinIO bucket that contains the Bronze objects.

Expected source purpose:

- expose the `football` bucket
- browse the Bronze folder structure
- provide file access for a metadata-driven semantic raw view

Target dataset name to create in Dremio:

- `semantic.raw_matches_odds`

This dataset is generated from the latest successful ingestion metadata and points at the Bronze files for that run.

The generated SQL artifact is:

- [semantic_raw_matches_odds.sql](/Users/valtercheque/Documents/Portfolio/Football-Betting-DE/dremio/sql/semantic_raw_matches_odds.sql)

The sync script that rebuilds it is:

- [sync_semantic_layer.py](/Users/valtercheque/Documents/Portfolio/Football-Betting-DE/infrastructure/scripts/sync_semantic_layer.py)

Target dataset name for curated match data:

- `semantic.silver_matches`

This dataset points at the latest published Silver Parquet artifact in MinIO.

The publisher script is:

- [publish_silver_matches.py](/Users/valtercheque/Documents/Portfolio/Football-Betting-DE/infrastructure/scripts/publish_silver_matches.py)

The Bronze source shape remains:

```text
football/bronze/source=football_data_co_uk/entity=matches_odds
```

### Source: PostgreSQL

Create a source for the platform PostgreSQL database.

Expected source purpose:

- expose `pipeline_run`
- expose `file_manifest`
- expose `dim_league`

Target dataset names to create in Dremio:

- `semantic.pipeline_run`
- `semantic.file_manifest`
- `semantic.dim_league`

## Why Stable Semantic Names Matter

The Bronze object path contains:

- `ingest_date`
- `run_id`
- `league`
- `season`

Those are useful for lineage and storage, but they are not good downstream dataset names.

dbt should depend on stable semantic names like:

- `raw_matches_odds`
- `silver_matches`
- `pipeline_run`
- `dim_league`

not directly on run-specific storage paths.

The semantic refresh process handles the run-specific path generation centrally.

## Manual Setup Sequence in Dremio

1. Open Dremio UI at `http://localhost:9047`
2. Log in with the local admin user
3. Add a source for MinIO
4. Add a source for PostgreSQL
5. Create a Space named `semantic`
6. Create or save stable datasets in that Space:
   - `raw_matches_odds`
   - `silver_matches`
   - `pipeline_run`
   - `file_manifest`
   - `dim_league`

After initial setup, `raw_matches_odds` should be refreshed by the sync script instead of manual editing.

## dbt Contract

The dbt project in this repository is written against the `semantic` space.

That means dbt expects these Dremio datasets to exist:

- `semantic.raw_matches_odds`
- `semantic.silver_matches`
- `semantic.pipeline_run`
- `semantic.file_manifest`
- `semantic.dim_league`

Current Gold outputs built in Dremio by dbt:

- `semantic.gold_match_context`
- `semantic.gold_h2h_context`

## Interview Summary

This layer shows:

- abstraction over raw storage paths
- separation between operational storage and analytical access
- a clean handoff from ingestion to transformation
