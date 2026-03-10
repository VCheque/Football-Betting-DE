# Local Stack Design

## Purpose

This document defines the first local platform stack for the MVP.

The goal is to decide exactly which services need to run, how they connect, and why they are included before writing `docker-compose.yml`.

## MVP Scope

The first runnable stack should support:

- local object storage for Bronze data
- relational storage for metadata and dimensions
- a semantic query layer
- one scheduler runtime
- one ingestion runtime
- one dbt runtime
- one application runtime

## Services

### 1. MinIO

Purpose:

- store Bronze raw files
- later store Silver and Gold datasets

Ports:

- `9000` for S3 API
- `9001` for MinIO console

Storage:

- persistent Docker volume or mounted local directory

Initial MVP use:

- store raw `matches_odds` CSV files in Bronze

### 2. PostgreSQL

Purpose:

- store ingestion metadata
- store dimensions and configuration

Ports:

- `5432`

Storage:

- persistent Docker volume or mounted local directory

Initial MVP use:

- store `pipeline_run`
- store `file_manifest`
- store `source_config`
- store `dim_league`
- store `dim_team`

### 3. Dremio

Purpose:

- query MinIO and PostgreSQL through one analytical layer

Ports:

- `9047` for Dremio UI and API

Storage:

- persistent Docker volume or mounted local directory

Initial MVP use:

- register MinIO as a source
- register PostgreSQL as a source
- provide a stable interface for dbt

### 4. Ingestion Runner

Purpose:

- execute Python ingestion jobs

Ports:

- no public port required

Initial MVP use:

- fetch source files
- upload raw files to MinIO
- write metadata to PostgreSQL

Execution style:

- one-off job container
- manual execution during MVP development

### 5. Scheduler

Purpose:

- trigger ingestion jobs on a fixed schedule

Ports:

- no public port required

Initial MVP use:

- run ingestion four times per day
- keep Bronze and metadata fresh
- show that jobs can run automatically at defined times

Execution style:

- always-on lightweight scheduler container
- cron-style execution

### 6. dbt Runner

Purpose:

- run dbt models and tests

Ports:

- no public port required

Initial MVP use:

- build `silver_matches`
- build `gold_match_context`
- execute dbt tests

Execution style:

- one-off job container
- manual execution during MVP development

### 7. Streamlit

Purpose:

- serve the demo application

Ports:

- `8501`

Initial MVP use:

- optional in the first infrastructure run
- included in the stack definition now so the final architecture remains visible

## Service Dependency Order

The stack dependency order should be:

1. PostgreSQL starts
2. MinIO starts
3. Dremio starts after PostgreSQL and MinIO are available
4. Ingestion runner starts after PostgreSQL and MinIO are available
5. Scheduler starts after PostgreSQL and MinIO are available
6. dbt runner starts after Dremio is available
7. Streamlit starts after Dremio is available

## Network Design

Use one internal Docker network.

Reason:

- services should communicate internally by service name
- local stack remains simple
- only required ports should be exposed to the host

## Volume Design

Persist the following data:

- MinIO object storage data
- PostgreSQL database files
- Dremio state

Reason:

- local restarts should not destroy the environment
- the platform should behave like a real persistent system

## Environment Variables

The first Compose setup should define variables for:

- PostgreSQL database name
- PostgreSQL user
- PostgreSQL password
- PostgreSQL port
- MinIO root user
- MinIO root password
- MinIO API port
- MinIO console port
- MinIO bucket name
- scheduler season settings
- Dremio port
- Streamlit port

## First Compose File Should Include

The first `docker-compose.yml` should define:

- `postgres`
- `minio`
- `dremio`
- `scheduler`
- `ingestion-runner`
- `dbt-runner`
- `streamlit`

It should also include:

- one shared network
- persistent volumes
- minimal health checks where practical

## What the First Compose File Should Not Include

Avoid adding these in the first version unless they are truly required:

- Airflow
- Prefect
- Kafka
- Spark
- Nginx
- TLS setup
- production backup jobs

Reason:

These increase complexity without helping the first MVP story.

## First Technical Goal

The first infrastructure milestone is successful when:

1. MinIO is reachable,
2. PostgreSQL is reachable,
3. Dremio starts,
4. containers can talk to each other on the internal network.

At that point, ingestion can be added as the first real data movement step.

## Interview Summary

This local stack is intentionally small but realistic.

It demonstrates:

- lake-style raw storage,
- relational metadata tracking,
- semantic federation,
- transformation as a separate runtime,
- application serving as a downstream concern.
