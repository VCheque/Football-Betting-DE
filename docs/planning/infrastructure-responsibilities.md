# Infrastructure Responsibilities

## Purpose

This document defines the responsibility of each platform component before implementation starts.

The goal is to make the local stack easy to explain and easy to justify in an interview.

## Core Principle

Each service should have one clear job.

This project should avoid:

- storing everything in one database,
- transforming data in the UI,
- mixing raw storage with metadata storage,
- using services without a clear reason.

## Local Platform Overview

The local MVP stack will use:

- MinIO
- PostgreSQL
- Dremio
- scheduler
- dbt runner
- ingestion runner
- Streamlit

The stack will run locally with Docker Compose.

## Service Responsibilities

### 1. MinIO

Role:

Object storage and dataset system of record.

What it stores:

- Bronze raw files
- Silver transformed files
- Gold curated files

Why it is needed:

- raw datasets should not live only in PostgreSQL
- object storage is the correct pattern for a lake-style architecture
- it supports replay, audit, and scalable storage

How it is used in the MVP:

- store raw match/odds CSV files in Bronze
- later store curated Parquet or Iceberg outputs

What it should not do:

- store operational metadata as the main system
- replace transformation logic

### 2. PostgreSQL

Role:

Control-plane and reference-data store.

What it stores:

- pipeline run metadata
- file manifests
- source configuration
- league dimensions
- team dimensions
- alias mappings
- data quality results

Why it is needed:

- metadata must be queryable and structured
- dimensions and config are better stored relationally than as flat files
- it makes ingestion and platform operations traceable

How it is used in the MVP:

- track each ingestion run
- store which files were loaded
- store canonical league/team metadata

What it should not do:

- become the main raw data lake
- replace Bronze object storage

### 3. Dremio

Role:

Federation and semantic query layer.

What it connects to:

- MinIO
- PostgreSQL

Why it is needed:

- it gives one query surface across file-based and relational sources
- it decouples downstream consumers from physical storage details
- it creates a cleaner path for dbt and Streamlit

How it is used in the MVP:

- query Bronze files in MinIO
- query metadata and dimensions in PostgreSQL
- expose stable views for dbt transformations

What it should not do:

- replace dbt as the main transformation framework
- replace PostgreSQL as metadata storage

### 4. dbt Runner

Role:

Transformation, testing, and documentation layer.

What it does:

- build Silver models
- build Gold models
- run data tests
- generate lineage and docs artifacts

Why it is needed:

- transformation logic should be explicit, testable, and versioned
- dbt is a strong interview signal for modern analytics engineering
- it keeps business logic out of ingestion and UI layers

How it is used in the MVP:

- build `silver_matches`
- build `gold_match_context`
- validate key constraints and accepted values

What it should not do:

- perform raw source extraction
- replace orchestration or storage

### 5. Ingestion Runner

Role:

Source extraction and raw data loading.

What it does:

- download source files
- assign run identifiers
- compute checksums and row counts
- upload raw files to MinIO Bronze
- write metadata into PostgreSQL

Why it is needed:

- extraction should be isolated from transformation
- raw ingestion needs its own execution boundary
- it provides a clean operational step for traceability

How it is used in the MVP:

- ingest `football-data.co.uk` match/odds season files

What it should not do:

- perform business aggregations
- become the main analytics layer

### 6. Streamlit

Role:

Serving and demonstration layer.

What it does:

- query curated datasets
- render dashboards and exploratory views
- present the final business-facing outputs

Why it is needed:

- it provides a visible end product for the portfolio
- it demonstrates that curated datasets are usable by applications

How it is used in the MVP:

- not in the first technical implementation step
- added after Silver and Gold datasets exist

What it should not do:

- ingest raw data
- perform heavy transformations
- rebuild analytics logic that belongs in dbt

### 7. Scheduler

Role:

Trigger ingestion jobs at specific times.

What it does:

- runs the ingestion command on a fixed daily schedule
- keeps Bronze and metadata fresh during the day
- demonstrates orchestration without introducing heavy tooling

Why it is needed:

- football data changes during the day
- the project needs a visible scheduling story for interviews
- a single-VM MVP does not need Airflow yet

How it is used in the MVP:

- run ingestion four times per day
- load the current season plus prior-season history

What it should not do:

- contain business transformation logic
- replace dbt
- become a complex workflow engine

## Service Interactions

### Data Flow

1. Ingestion runner reads from external sources.
2. Ingestion runner writes raw files to MinIO Bronze.
3. Ingestion runner writes run metadata to PostgreSQL.
4. Dremio connects to MinIO and PostgreSQL.
5. dbt reads through Dremio and builds Silver/Gold datasets.
6. Curated datasets are stored back in MinIO.
7. Streamlit reads curated datasets through Dremio.

## Why This Stack Is Good for the MVP

- it demonstrates modern DE architecture without excessive complexity
- each service has a clear responsibility
- it supports a clean raw-to-curated story
- it is realistic enough for production discussion
- it is still small enough to run locally

## What Will Be Implemented First

The first technical implementation should focus on:

1. infrastructure startup,
2. first ingestion into Bronze,
3. PostgreSQL metadata tracking.

dbt and Streamlit come after the raw and metadata layers are proven.

## Interview Summary

This infrastructure design shows that:

- raw data storage and metadata storage are intentionally separated,
- transformation is treated as its own layer,
- application serving is downstream of curated data,
- the stack is organized around responsibility, not around convenience.
