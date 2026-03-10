#!/usr/bin/env bash

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

  echo "$(timestamp) scheduled-pipeline starting season_start=${season_start} history_seasons=${history_seasons}"

  set -- python -m src.main --season-start "${season_start}" --history-seasons "${history_seasons}"

  if [ -n "${INGEST_LEAGUE_CODE:-}" ]; then
    set -- "$@" --league-code "${INGEST_LEAGUE_CODE}"
  fi

  (
    cd /workspace/ingestion
    "$@"
  )

  (
    cd /workspace
    python infrastructure/scripts/sync_semantic_layer.py
  )

  (
    cd /workspace
    python infrastructure/scripts/publish_silver_matches.py
  )

  (
    cd /workspace/dbt
    dbt run --profiles-dir /workspace/dbt
    dbt test --profiles-dir /workspace/dbt
  )

  echo "$(timestamp) scheduled-pipeline completed"
}

main "$@"
