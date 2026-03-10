# Build Order

## Purpose

This document defines the implementation order for the first MVP so the project is built in a way that is easy to explain end to end.

## First MVP Slice

- Source: `football-data.co.uk`
- Entity: `matches_odds`
- Bronze output: raw season CSV files
- Silver output: `silver_matches`
- Gold output: `gold_match_context`
- Metadata store: PostgreSQL
- Semantic/query layer: Dremio

## Build Sequence

### Step 1. Define the MVP contract

Define:

- business goal
- source
- entity
- grain
- Bronze structure
- Silver output
- Gold output
- metadata tables
- quality checks

Reason:

This prevents building ingestion without knowing what the pipeline must produce.

### Step 2. Define the repository structure

Define the folder layout for:

- ingestion
- PostgreSQL SQL
- dbt
- Dremio config
- documentation
- Streamlit

Reason:

This separates responsibilities early and avoids another app-centric layout.

### Step 3. Define infrastructure responsibilities

Define what each platform component is responsible for:

- MinIO stores datasets
- PostgreSQL stores metadata and dimensions
- Dremio federates sources
- dbt transforms data
- Streamlit consumes curated outputs

Reason:

This is the architecture foundation for all implementation decisions.

### Step 4. Stand up the local platform

Create Docker Compose for:

- MinIO
- PostgreSQL
- Dremio
- dbt runner
- ingestion runner
- Streamlit

Reason:

This creates the operating environment for the MVP.

### Step 5. Build the first ingestion pipeline

Ingest `matches_odds` raw files into MinIO Bronze.

Reason:

This establishes the raw layer and the first reproducible ingestion path.

### Step 6. Add PostgreSQL metadata tracking

Insert:

- pipeline runs
- file manifests
- source configuration
- league/team dimensions

Reason:

This makes the platform operationally traceable.

### Step 7. Connect MinIO and PostgreSQL to Dremio

Create the first analytical access layer across raw and metadata systems.

Reason:

This gives dbt and the future app one stable query surface.

### Step 8. Build dbt Silver models

Create `silver_matches`.

Reason:

This turns raw source data into a clean reusable dataset.

### Step 9. Build dbt Gold models

Create `gold_match_context`.

Reason:

This produces a business-ready dataset for analytics and demos.

### Step 10. Connect Streamlit to curated data

Use Dremio-backed curated datasets only.

Reason:

The app should serve data, not transform it.

### Step 11. Add scheduling and observability

Track:

- job execution
- data freshness
- dbt runs
- quality checks

Reason:

This shows reliability and production thinking in interviews.

## Guiding Principle

At every stage, be able to explain:

- what data enters,
- where it is stored,
- what changes are applied,
- what output is produced,
- how quality is verified.
