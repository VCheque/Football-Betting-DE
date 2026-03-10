# Dremio UI Setup

## Purpose

This runbook explains the first manual Dremio setup for the MVP.

This step is manual on purpose so the semantic-layer contract is visible and explainable.

## Before You Start

Make sure these services are already running:

- PostgreSQL
- MinIO
- Dremio

Current local Dremio login:

- username: `admin`
- password: `admin12345`

Open:

```text
http://localhost:9047
```

## Step 1. Create the PostgreSQL Source

Create a new source:

- Source type: `PostgreSQL`
- Source name: `postgres_meta`

Use these values:

- Host: `postgres`
- Port: `5432`
- Database: `football_platform`
- Username: `football`
- Password: `football`

After saving, verify you can browse:

- `pipeline_run`
- `file_manifest`
- `dim_league`

## Step 2. Create the MinIO Source

Create a new source:

- Source type: `S3`
- Source name: `bronze_minio`

Use these values:

- Access key: `minioadmin`
- Secret key: `minioadmin123`
- Bucket list: `football`

If Dremio shows an option for secure connection:

- set `Secure` to `false`

If Dremio shows an endpoint or compatibility setting for S3-compatible storage:

- point it to MinIO on the internal Docker network
- use `http://minio:9000`

After saving, verify you can browse:

```text
football/bronze/source=football_data_co_uk/entity=matches_odds
```

## Step 3. Create the `semantic` Space

Create a Dremio space named:

```text
semantic
```

This is where stable datasets will be saved for dbt.

## Step 4. Save Stable Datasets

Create and save the following datasets in the `semantic` space.

### Dataset 1. `semantic.raw_matches_odds`

Purpose:

- stable raw contract for dbt staging models

It should point to the Bronze `matches_odds` files for:

```text
football/bronze/source=football_data_co_uk/entity=matches_odds
```

### Dataset 2. `semantic.pipeline_run`

Purpose:

- expose ingestion run metadata

Source:

- `postgres_meta.public.pipeline_run`

### Dataset 3. `semantic.file_manifest`

Purpose:

- expose file-level load metadata

Source:

- `postgres_meta.public.file_manifest`

### Dataset 4. `semantic.dim_league`

Purpose:

- expose league reference data

Source:

- `postgres_meta.public.dim_league`

## Why These Dataset Names Matter

dbt is written against these stable dataset names:

- `semantic.raw_matches_odds`
- `semantic.pipeline_run`
- `semantic.file_manifest`
- `semantic.dim_league`

This keeps dbt independent from:

- raw run IDs
- deep Bronze folder paths
- direct PostgreSQL table references

## Expected Result

When this runbook is complete:

- Dremio can browse raw Bronze data
- Dremio can browse metadata tables
- the semantic layer exposes stable names for dbt

## Next Step

After this setup, the next command is to start the dbt runner and execute:

```text
dbt debug
dbt run
dbt test
```
