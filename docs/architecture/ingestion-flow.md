# Ingestion Flow

## Purpose

This document explains the first ingestion step for the MVP source: `football-data.co.uk` `matches_odds`.

## What the First Ingestion Step Does

The current ingestion implementation writes to the platform for the agreed 6 leagues.

It:

- downloads source CSV files,
- stores the downloaded files locally first,
- uploads them to MinIO Bronze,
- inserts run metadata into PostgreSQL,
- inserts file manifests into PostgreSQL,
- upserts league reference data into PostgreSQL,
- publishes a curated `silver_matches` Parquet artifact after ingestion.

## Inputs

- `league_code`
- `season_start`
- `history_seasons`

Example:

- `league_code = E0`
- `season_start = 2025`
- `history_seasons = 1`

This example loads:

- `2025/2026`
- `2024/2025`

## Ingestion Actions

1. Build the source URL from league code and season.
2. Download the raw CSV file.
3. Save the raw file to the local landing zone under `data/raw/...`.
4. Generate a `run_id`.
5. Compute the file checksum.
6. Count the data rows in the CSV.
7. Build the Bronze object key.
8. Build the `pipeline_run` payload.
9. Build the `file_manifest` payload.

## Local Landing Zone

Raw files are staged locally before object storage upload.

Path shape:

```text
data/raw/source=football_data_co_uk/entity=matches_odds/ingest_date=YYYY-MM-DD/run_id=<uuid>/league=<code>/season=<yyyy>/file.csv
```

This local step is useful because it:

- mirrors how many real ingestion jobs work,
- gives you a debuggable checkpoint before object storage,
- makes it easier to re-run uploads without re-downloading files,
- supports loading multiple seasons for H2H and historical features.

## Current Output

The script prints a JSON payload containing:

- `pipeline_run`
- the selected league and season combinations
- local staged file paths
- all `file_manifests`
- whether the command was run in `dry_run` mode

## Why This Stage Exists

This stage proves that the project can:

- identify a source file,
- land raw source files locally first,
- generate a reproducible run identifier,
- compute ingestion metadata,
- define the Bronze path contract,
- upload raw files to object storage,
- persist operational metadata in PostgreSQL.

## Next Ingestion Step

The next improvement will be:

1. insert team dimensions and alias mappings,
2. add ingestion tests,
3. build additional Gold datasets on top of `silver_matches`,
4. connect the serving layer to curated semantic datasets.
