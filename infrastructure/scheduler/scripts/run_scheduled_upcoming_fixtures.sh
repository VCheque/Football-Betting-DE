#!/usr/bin/env bash
# Every-2-hour ESPN upcoming-fixtures ingestion.
# Runs from the scheduler container (supercronic) every 2 hours.
# Mirrors the structure of run_scheduled_player_stats.sh.

set -eu

timestamp() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

main() {
  export PYTHONPATH="/workspace:${PYTHONPATH:-}"

  lookahead_days="${UPCOMING_FIXTURES_LOOKAHEAD_DAYS:-14}"

  echo "$(timestamp) upcoming-fixtures-pipeline starting lookahead_days=${lookahead_days}"

  # Fetch upcoming fixtures from ESPN and upload to MinIO Bronze
  (
    cd /workspace/ingestion
    python -m src.run_upcoming_fixtures \
      --lookahead-days "${lookahead_days}"
  )

  # Rebuild the Dremio semantic view for raw_upcoming_fixtures
  (
    cd /workspace
    python infrastructure/scripts/sync_semantic_layer.py \
      --source-name espn \
      --entity-name upcoming_fixtures \
      --dataset-name raw_upcoming_fixtures \
      --output-sql dremio/sql/semantic_raw_upcoming_fixtures.sql
  )

  # Run dbt to refresh the upcoming fixtures models
  (
    cd /workspace/dbt
    dbt run --profiles-dir /workspace/dbt --select stg_upcoming_fixtures silver_upcoming_fixtures
    dbt test --profiles-dir /workspace/dbt --select stg_upcoming_fixtures silver_upcoming_fixtures
  )

  echo "$(timestamp) upcoming-fixtures-pipeline completed"
}

main "$@"
