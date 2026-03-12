"""Data loading functions that query Dremio Gold models instead of local CSV files.

These functions are drop-in replacements for the CSV-based loaders in the
original app.py.  Each function returns a pandas DataFrame with the same
column names and dtypes that the rest of the app expects, so no other
app logic needs to change.

Mapping:
    load_matches()       → replaces load_data(DEFAULT_DATA_FILE)
    load_player_stats()  → replaces load_player_stats() / PLAYER_STATS_FILE
    load_injuries()      → replaces _read_optional_csv(injuries_file)
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from dremio_client import query


# ─────────────────────────────────────────────────────────────────────────────
# Match / historical data
# ─────────────────────────────────────────────────────────────────────────────

def load_matches(as_of: date | None = None) -> pd.DataFrame:
    """Load historical match data from silver_matches (via Dremio semantic layer).

    Returns a DataFrame with snake_case column names that match what
    generate_bet_combinations.py expects internally (its load_data() function
    already renames FDC raw columns to this format when reading from CSV).

    Args:
        as_of: If provided, only return matches up to this date (inclusive).
    """
    date_filter = ""
    if as_of is not None:
        date_filter = f"WHERE match_date <= DATE '{as_of.isoformat()}'"

    # All column names come through as-is from silver_matches (already snake_case).
    # Avoid SQL aliases to prevent reserved keyword issues (e.g. "AS" for away_shots).
    sql = f"""
        SELECT
            match_date,
            home_team,
            away_team,
            result_ft,
            home_goals_ft,
            away_goals_ft,
            home_goals_ht,
            away_goals_ht,
            home_shots,
            away_shots,
            home_shots_on_target,
            away_shots_on_target,
            home_fouls,
            away_fouls,
            home_corners,
            away_corners,
            home_yellow_cards,
            away_yellow_cards,
            home_red_cards,
            away_red_cards,
            odds_b365_home,
            odds_b365_draw,
            odds_b365_away,
            odds_pinnacle_home,
            odds_pinnacle_draw,
            odds_pinnacle_away,
            odds_avg_home,
            odds_avg_draw,
            odds_avg_away,
            league_code,
            season_label,
            CASE league_code
                WHEN 'E0'  THEN 'Premier League'
                WHEN 'SP1' THEN 'La Liga'
                WHEN 'I1'  THEN 'Serie A'
                WHEN 'D1'  THEN 'Bundesliga'
                WHEN 'F1'  THEN 'Ligue 1'
                WHEN 'P1'  THEN 'Primeira Liga'
                ELSE league_code
            END AS league_name
        FROM semantic.silver_matches
        {date_filter}
        ORDER BY match_date
    """
    df = query(sql)
    df["match_date"] = pd.to_datetime(df["match_date"])
    return df


def load_match_context(
    league_code: str | None = None,
    as_of: date | None = None,
) -> pd.DataFrame:
    """Load rolling form features from gold_match_context.

    Args:
        league_code: Optional football-data.co.uk league code filter (e.g. "E0").
        as_of:       Only return rows up to this date inclusive.
    """
    filters: list[str] = []
    if league_code:
        filters.append(f"league_code = '{league_code}'")
    if as_of:
        filters.append(f"match_date <= DATE '{as_of.isoformat()}'")

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = f"SELECT * FROM semantic.gold_match_context {where} ORDER BY match_date"
    df = query(sql)
    df["match_date"] = pd.to_datetime(df["match_date"])
    return df


def load_h2h_context(
    home_team: str,
    away_team: str,
    league_code: str | None = None,
) -> pd.DataFrame:
    """Load head-to-head analytics for a specific matchup from gold_h2h_context."""
    filters = [
        f"home_team = '{home_team}'",
        f"away_team = '{away_team}'",
    ]
    if league_code:
        filters.append(f"league_code = '{league_code}'")
    where = "WHERE " + " AND ".join(filters)
    sql = f"SELECT * FROM semantic.gold_h2h_context {where} ORDER BY match_date DESC LIMIT 1"
    df = query(sql)
    if not df.empty:
        df["match_date"] = pd.to_datetime(df["match_date"])
    return df


def load_standings(
    league_code: str | None = None,
    season_label: str | None = None,
) -> pd.DataFrame:
    """Load season standings from gold_standings.

    Args:
        league_code:  Filter by league (e.g. "E0" for Premier League).
        season_label: Filter by season (e.g. "2024/2025").
    """
    filters: list[str] = []
    if league_code:
        filters.append(f"league_code = '{league_code}'")
    if season_label:
        filters.append(f"season_label = '{season_label}'")

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = f"SELECT * FROM semantic.gold_standings {where} ORDER BY position"
    return query(sql)


def load_rest_fatigue(
    league_code: str | None = None,
    as_of: date | None = None,
) -> pd.DataFrame:
    """Load rest and fatigue indicators from gold_rest_fatigue."""
    filters: list[str] = []
    if league_code:
        filters.append(f"league_code = '{league_code}'")
    if as_of:
        filters.append(f"match_date <= DATE '{as_of.isoformat()}'")

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = f"SELECT * FROM semantic.gold_rest_fatigue {where} ORDER BY match_date"
    df = query(sql)
    if not df.empty:
        df["match_date"] = pd.to_datetime(df["match_date"])
    return df


def load_team_season_stats(
    league_code: str | None = None,
    season_label: str | None = None,
    team: str | None = None,
    scope: str | None = None,
) -> pd.DataFrame:
    """Load team season stats from gold_team_season_stats.

    Args:
        scope: One of 'all', 'home', 'away', 'vs_top_half', 'vs_bottom_half'.
    """
    filters: list[str] = []
    if league_code:
        filters.append(f"league_code = '{league_code}'")
    if season_label:
        filters.append(f"season_label = '{season_label}'")
    if team:
        filters.append(f"team = '{team}'")
    if scope:
        filters.append(f"scope = '{scope}'")

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = f"SELECT * FROM semantic.gold_team_season_stats {where}"
    return query(sql)


# ─────────────────────────────────────────────────────────────────────────────
# Player stats (Understat — 5 leagues, no Primeira Liga)
# ─────────────────────────────────────────────────────────────────────────────

def load_player_stats(
    league_code: str | None = None,
    season_label: str | None = None,
    team: str | None = None,
) -> pd.DataFrame:
    """Load player season statistics from gold_player_stats (replaces player_stats.csv).

    Column names are normalised to match what app.py and xgboost_models.py expect:
        player  (← player_name)
        matches (← games)
        minutes (← minutes_played)
    All other columns pass through unchanged.
    """
    filters: list[str] = []
    if league_code:
        filters.append(f"league_code = '{league_code}'")
    if season_label:
        filters.append(f"season_label = '{season_label}'")
    if team:
        filters.append(f"team = '{team}'")

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = f"SELECT * FROM semantic.gold_player_stats {where}"
    df = query(sql)
    return df.rename(columns={
        "player_name": "player",
        "games": "matches",
        "minutes_played": "minutes",
    })


# ─────────────────────────────────────────────────────────────────────────────
# Injuries (manually uploaded via upload_injuries.py)
# ─────────────────────────────────────────────────────────────────────────────

def load_derby_pairs() -> set[frozenset[str]]:
    """Load active derby / same-city rivalry pairs from the dbt seed.

    Queries ``semantic.derby_pairs`` (materialised by ``dbt seed``) and
    returns a set of frozensets so ``xgboost_models.is_derby()`` can use it.
    Falls back to an empty set if the table has not been seeded yet.

    Bidirectional matching is guaranteed by frozenset: a pair stored as
    (Arsenal, Tottenham) matches both Arsenal-home and Tottenham-home fixtures.
    """
    sql = "SELECT team_a, team_b FROM semantic.derby_pairs"
    try:
        df = query(sql)
        return {frozenset({str(r.team_a), str(r.team_b)}) for _, r in df.iterrows()}
    except Exception:  # noqa: BLE001
        # Graceful degradation: seed may not have been run yet
        return set()


# ─────────────────────────────────────────────────────────────────────────────
# Injuries (manually uploaded via upload_injuries.py)
# ─────────────────────────────────────────────────────────────────────────────

def load_injuries(
    league_code: str | None = None,
    team: str | None = None,
) -> pd.DataFrame:
    """Load active injury records from gold_injuries (replaces external/injuries.csv).

    Returns columns: player_name, team, league_code, injury_type,
    return_date, indefinite_return.

    Returns an empty DataFrame if the source has not been populated yet.
    """
    filters: list[str] = []
    if league_code:
        filters.append(f"league_code = '{league_code}'")
    if team:
        filters.append(f"team = '{team}'")

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = f"SELECT * FROM semantic.gold_injuries {where}"
    try:
        df = query(sql)
        if "return_date" in df.columns:
            df["return_date"] = pd.to_datetime(df["return_date"], errors="coerce")
        return df
    except Exception:  # noqa: BLE001
        # Graceful degradation: injuries dataset may not exist yet
        return pd.DataFrame(
            columns=["player_name", "team", "league_code", "injury_type", "return_date", "indefinite_return"]
        )
