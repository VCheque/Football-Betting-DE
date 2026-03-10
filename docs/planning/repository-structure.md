# Repository Structure

## Purpose

This document defines how the repository should be organized before implementation starts.

The goal is to separate platform responsibilities clearly so the project is easy to explain in an interview and easy to extend over time.

## Design Principle

Each folder should answer one question:

- where does ingestion code live?
- where does infrastructure setup live?
- where do SQL definitions live?
- where do dbt models live?
- where does the app live?
- where do planning and architecture docs live?

This avoids the previous app-centric structure where data access, transformation, and UI logic were mixed together.

## Proposed Top-Level Structure

```text
.
├── README.md
├── docs/
├── infrastructure/
├── ingestion/
├── sql/
├── dbt/
├── dremio/
├── streamlit/
└── tests/
```

## Folder Responsibilities

### `docs/`

Purpose:

- planning
- architecture decisions
- MVP definitions
- operating notes

Why it exists:

This is where the project narrative lives. In an interview, this folder helps explain the reasoning before showing implementation.

Suggested subfolders:

```text
docs/
├── planning/
└── architecture/
```

### `infrastructure/`

Purpose:

- Docker Compose files
- environment templates
- container bootstrap files
- local platform setup

Why it exists:

Infrastructure should be separated from business logic and transformation logic.

Suggested contents:

```text
infrastructure/
├── docker-compose.yml
├── .env.example
└── bootstrap/
```

### `ingestion/`

Purpose:

- source extraction jobs
- raw file loading logic
- ingestion utilities
- run metadata capture

Why it exists:

This is the layer responsible for moving data from external systems into the platform.

Suggested contents:

```text
ingestion/
├── src/
├── config/
├── runners/
└── requirements.txt
```

### `sql/`

Purpose:

- PostgreSQL schema definitions
- metadata table DDL
- seed or setup SQL

Why it exists:

Operational SQL should stay separate from dbt transformation logic.

Suggested contents:

```text
sql/
└── postgres/
```

### `dbt/`

Purpose:

- dbt project configuration
- staging, Silver, and Gold models
- tests
- seeds

Why it exists:

This is the transformation and data quality layer.

Suggested contents:

```text
dbt/
├── models/
├── seeds/
├── tests/
├── dbt_project.yml
└── profiles.yml
```

### `dremio/`

Purpose:

- source registration notes
- semantic layer setup
- Dremio bootstrap assets if needed later

Why it exists:

Dremio is its own platform concern and should not be hidden inside dbt or app code.

Suggested contents:

```text
dremio/
└── README.md
```

### `streamlit/`

Purpose:

- application code
- dashboard components
- semantic dataset consumption

Why it exists:

The app should be a thin serving layer, clearly separated from ingestion and transformation.

Suggested contents:

```text
streamlit/
├── app.py
└── pages/
```

### `tests/`

Purpose:

- integration checks
- ingestion tests
- utility tests

Why it exists:

dbt handles dataset tests, but Python code should still have its own tests.

Suggested contents:

```text
tests/
├── ingestion/
└── integration/
```

## What Should Not Exist

The following patterns should be avoided:

- one large root-level `app.py` that mixes everything
- local `data/` folders acting as the platform storage layer
- business logic embedded directly in Streamlit
- metadata stored in local JSON files when it belongs in PostgreSQL
- transformation logic embedded inside ingestion scripts

## First Folders Needed for the MVP

For the first implementation slice, only these are required:

```text
docs/
infrastructure/
ingestion/
sql/postgres/
dbt/
dremio/
streamlit/
```

The `tests/` folder can be created early, but it is not required before the first ingestion flow exists.

## Recommended Build Sequence After This

1. Create the folder structure.
2. Add infrastructure files.
3. Add PostgreSQL schema files.
4. Add the first ingestion job.
5. Add the dbt project.
6. Add the Streamlit serving layer last.

## Interview Summary

This structure shows:

- separation of concerns
- data platform thinking instead of app-first thinking
- clear boundaries between ingestion, storage, modeling, and serving
- a repository layout that can scale as more sources and datasets are added
