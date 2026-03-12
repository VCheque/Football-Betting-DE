#!/usr/bin/env bash
# Weekly Understat player-stats ingestion.
# Runs from the scheduler container (supercronic) every Sunday at 02:00 UTC.
# Mirrors the structure of run_scheduled_ingestion.sh.

set -eu

timestamp() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

resolve_season_start() {
  if [ -n "${INGEST_SEASON_START:-}" ]; then
    printf "%s\n" "${INGEST_SEASON_START}"
    return
  fi

  current_year="$(date -u +%Y)"
  current_month="$(date -u +%m)"
  rollover_month="${SEASON_ROLLOVER_MONTH:-8}"

  if [ "${current_month#0}" -ge "${rollover_month#0}" ]; then
    printf "%s\n" "${current_year}"
  else
    printf "%s\n" "$((current_year - 1))"
  fi
}

main() {
  export PYTHONPATH="/workspace:${PYTHONPATH:-}"

  season_start="$(resolve_season_start)"
  history_seasons="${INGEST_HISTORY_SEASONS:-1}"

  echo "$(timestamp) player-stats-pipeline starting season_start=${season_start} history_seasons=${history_seasons}"

  # Fetch player stats from Understat and upload to MinIO Bronze
  (
    cd /workspace/ingestion
    python -m src.run_player_stats \
      --season-start "${season_start}" \
      --history-seasons "${history_seasons}"
  )

  # Rebuild the Dremio semantic view for raw_player_stats
  (
    cd /workspace
    python infrastructure/scripts/sync_semantic_layer.py \
      --source-name understat \
      --entity-name player_stats \
      --dataset-name raw_player_stats \
      --output-sql dremio/sql/semantic_raw_player_stats.sql
  )

  # Run dbt to refresh the player stats models
  (
    cd /workspace/dbt
    dbt run --profiles-dir /workspace/dbt --select stg_player_stats gold_player_stats
    dbt test --profiles-dir /workspace/dbt --select stg_player_stats gold_player_stats
  )

  echo "$(timestamp) player-stats-pipeline completed"
}

main "$@"
