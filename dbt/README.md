# dbt

This folder contains the transformation layer for the project.

## Current State

The project now includes:

- a `dbt_project.yml`
- a profile example
- staging models
- the first Gold model
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

- `stg_matches_odds`
- `stg_pipeline_run`

### Gold

- `gold_match_context`

## Responsibility

- standardize raw data into analytics-friendly columns
- define tested models for the portfolio pipeline
- keep transformation logic out of ingestion and Streamlit
