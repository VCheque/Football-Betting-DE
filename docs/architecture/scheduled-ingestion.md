# Scheduled Ingestion

## Purpose

This document explains how the platform refresh job is scheduled to run automatically four times per day.

This part of the project exists for two reasons:

1. show that the platform can run jobs on a controlled schedule,
2. keep match and standings inputs fresh because football data changes during the day.

## Why Four Runs Per Day

The source data is not static.

Match results, postponed fixtures, and league-table inputs can change throughout the day.
Running ingestion four times per day is a simple and credible MVP choice because it:

- shows orchestration capability,
- improves freshness without introducing orchestration complexity,
- is easy to explain in an interview,
- matches the single-VM Docker Compose scope of the project.

## Schedule

The scheduler runs at these UTC times every day:

- `00:15`
- `06:15`
- `12:15`
- `18:15`

Cron expression:

```text
15 0,6,12,18 * * *
```

The schedule is defined in:

- [ingestion-crontab](/Users/valtercheque/Documents/Portfolio/Football-Betting-DE/infrastructure/scheduler/cron/ingestion-crontab)

## Scheduler Implementation

The project uses a dedicated `scheduler` service in Docker Compose.

This service:

- runs inside the same Docker network as PostgreSQL, MinIO, and Dremio,
- uses cron syntax through `supercronic`,
- triggers the ingestion Python job automatically,
- rebuilds the semantic raw dataset from PostgreSQL metadata,
- runs dbt models and tests after the semantic refresh,
- logs execution output to the container logs.

Relevant files:

- [docker-compose.yml](/Users/valtercheque/Documents/Portfolio/Football-Betting-DE/infrastructure/docker-compose.yml)
- [scheduler Dockerfile](/Users/valtercheque/Documents/Portfolio/Football-Betting-DE/infrastructure/scheduler/Dockerfile)
- [run_scheduled_ingestion.sh](/Users/valtercheque/Documents/Portfolio/Football-Betting-DE/infrastructure/scheduler/scripts/run_scheduled_ingestion.sh)

## What the Scheduled Job Runs

The scheduler executes three steps:

```text
python -m src.main --season-start <resolved_year> --history-seasons <n>
python infrastructure/scripts/sync_semantic_layer.py
dbt run && dbt test
```

By default it loads:

- the current season,
- one prior season for history and H2H use cases.

## Automatic Season Resolution

The scheduler resolves the current season start automatically.

Logic:

- if the current UTC month is August or later, use the current year,
- otherwise use the previous year.

Examples:

- March 10, 2026 -> `2025/2026`
- September 10, 2026 -> `2026/2027`

This avoids hardcoding the season in the scheduled job.

## Environment Variables

The scheduler uses these settings from:

- [infrastructure/.env.example](/Users/valtercheque/Documents/Portfolio/Football-Betting-DE/infrastructure/.env.example)

Main variables:

- `INGEST_HISTORY_SEASONS`
- `SEASON_ROLLOVER_MONTH`
- `INGEST_LEAGUE_CODE`

Behavior:

- `INGEST_HISTORY_SEASONS=1` means current season plus one prior season
- `SEASON_ROLLOVER_MONTH=8` means the season changes in August
- `INGEST_LEAGUE_CODE` is blank by default, so all six leagues are loaded

## Data Flow of a Scheduled Run

1. The scheduler triggers the ingestion wrapper script.
2. The wrapper resolves the season year automatically.
3. The ingestion job downloads raw CSV files locally into `ingestion/data/raw/...`.
4. The same files are uploaded to MinIO Bronze.
5. Run metadata is written to PostgreSQL.
6. File metadata is written to PostgreSQL.
7. The semantic raw SQL is rebuilt from the latest successful `pipeline_run` and `file_manifest` rows.
8. Dremio `semantic.raw_matches_odds` is recreated from that generated SQL.
9. dbt builds `silver_matches_physical` as the dbt-owned Silver table.
10. dbt exposes `semantic.silver_matches` as the stable semantic Silver dataset.
11. dbt builds the Gold models.
12. dbt tests the refreshed datasets.

## Operational Outcome

Each scheduled run creates:

- a new `run_id`
- new raw snapshots in Bronze
- refreshed dbt-owned Silver and Gold datasets
- new `pipeline_run` metadata
- new `file_manifest` records

This is intentional.

The raw layer is append-only and versioned by run, which supports:

- replay,
- auditing,
- comparing snapshots over time.

## Metadata-Driven Semantic Refresh

The semantic raw dataset is not hardcoded to one historical `run_id`.

Instead, the sync step:

- finds the latest successful ingestion run in PostgreSQL,
- reads its file manifests,
- rebuilds [semantic_raw_matches_odds.sql](/Users/valtercheque/Documents/Portfolio/Football-Betting-DE/dremio/sql/semantic_raw_matches_odds.sql),
- republishes `semantic.raw_matches_odds` in Dremio.

This keeps downstream dbt models aligned with the latest Bronze snapshot.

## dbt-Owned Silver

The Silver layer is no longer published by a Python script.

Instead:

- Python owns ingestion into Bronze
- the semantic raw layer exposes the latest successful Bronze run
- dbt owns the transformation from raw semantic data into `silver_matches`
- Gold models depend on `ref('silver_matches')`

This keeps transformation ownership in one framework and makes lineage, tests, and model dependencies explicit.

## Interview Explanation

A clear way to explain this design:

- the source changes during the day, so one daily load is too coarse,
- four runs per day is enough to show freshness and scheduling without overengineering,
- cron syntax is explicit and easy to reason about,
- the scheduler is isolated from ingestion logic,
- the semantic layer follows metadata instead of a hardcoded run,
- the ingestion job remains reusable for manual runs and scheduled runs.
