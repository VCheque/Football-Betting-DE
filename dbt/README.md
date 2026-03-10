# dbt

This folder contains the transformation layer for the project.

## Current State

The project now includes:

- a `dbt_project.yml`
- a profile example
- staging models
- a Silver model owned by dbt
- Gold models
- model tests

## Current Contract

The dbt project expects Dremio datasets in the `semantic` space:

- `semantic.raw_matches_odds`
- `semantic.pipeline_run`
- `semantic.file_manifest`
- `semantic.dim_league`

These should be created in Dremio before running dbt.

## First Models

### Staging

- `stg_raw_matches_odds`
- `stg_pipeline_run`

### Silver

- `silver_matches_physical`
- `silver_matches`

### Gold

- `gold_match_context`
- `gold_h2h_context`

## Responsibility

- standardize raw semantic data into Silver
- define tested Gold models for the portfolio pipeline
- keep transformation logic out of ingestion and Streamlit
