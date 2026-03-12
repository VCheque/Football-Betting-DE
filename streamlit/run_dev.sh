#!/bin/bash
# Run the app locally without Dremio by falling back to the Football Bets CSVs.
# Usage: ./run_dev.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FB_DATA="/Users/valtercheque/Documents/Portfolio/Football Bets/data/sports/processed"

MATCHES_CSV="${FB_DATA}/top6_plus_portugal_matches_odds_since2022.csv" \
PLAYER_STATS_CSV="${FB_DATA}/player_stats.csv" \
streamlit run "${SCRIPT_DIR}/app.py"
