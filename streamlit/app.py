from __future__ import annotations

import os
import time
from datetime import date
from typing import Any

import pandas as pd
import requests
import streamlit as st


DREMIO_HOST = os.getenv("DREMIO_HOST", "localhost")
DREMIO_PORT = os.getenv("DREMIO_PORT", "9047")
DREMIO_USER = os.getenv("DREMIO_USER", "admin")
DREMIO_PASSWORD = os.getenv("DREMIO_PASSWORD", "admin12345")
DREMIO_BASE_URL = f"http://{DREMIO_HOST}:{DREMIO_PORT}"


def _quote(value: str) -> str:
    return value.replace("'", "''")


@st.cache_data(ttl=300, show_spinner=False)
def dremio_token() -> str:
    response = requests.post(
        f"{DREMIO_BASE_URL}/apiv2/login",
        json={"userName": DREMIO_USER, "password": DREMIO_PASSWORD},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    return payload["token"]


def wait_for_job(job_id: str, token: str) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {token}"}
    job_url = f"{DREMIO_BASE_URL}/api/v3/job/{job_id}"

    for _ in range(60):
        response = requests.get(job_url, headers=headers, timeout=30)
        response.raise_for_status()
        payload = response.json()
        job_state = payload.get("jobState")
        if job_state == "COMPLETED":
            return payload
        if job_state in {"CANCELED", "FAILED"}:
            raise RuntimeError(payload.get("errorMessage") or f"Dremio job failed: {job_state}")
        time.sleep(0.5)

    raise TimeoutError(f"Dremio job {job_id} did not complete in time")


def fetch_job_results(job_id: str, token: str, limit: int = 500) -> list[dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}"}
    rows: list[dict[str, Any]] = []
    offset = 0

    while True:
        response = requests.get(
            f"{DREMIO_BASE_URL}/api/v3/job/{job_id}/results",
            headers=headers,
            params={"limit": limit, "offset": offset},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        batch = payload.get("rows", [])
        rows.extend(batch)
        if len(batch) < limit:
            break
        offset += limit

    return rows


@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    token = dremio_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    response = requests.post(
        f"{DREMIO_BASE_URL}/api/v3/sql",
        headers=headers,
        json={"sql": sql},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    job_id = payload["id"]
    wait_for_job(job_id, token)
    rows = fetch_job_results(job_id, token)
    return pd.DataFrame(rows)


@st.cache_data(ttl=300, show_spinner=False)
def overview_metrics() -> dict[str, Any]:
    summary_sql = """
        select
            count(*) as matches_loaded,
            count(distinct league_code) as leagues_loaded,
            count(distinct season_label) as seasons_loaded,
            min(match_date) as first_match_date,
            max(match_date) as last_match_date
        from semantic.silver_matches
    """
    pipeline_sql = """
        select
            run_id,
            status,
            row_count,
            completed_at
        from semantic.pipeline_run
        order by completed_at desc
        limit 1
    """
    summary = run_query(summary_sql)
    pipeline = run_query(pipeline_sql)
    return {
        "summary": summary.iloc[0].to_dict() if not summary.empty else {},
        "pipeline": pipeline.iloc[0].to_dict() if not pipeline.empty else {},
    }


@st.cache_data(ttl=300, show_spinner=False)
def available_leagues() -> list[str]:
    data = run_query(
        """
        select distinct league_code
        from semantic.silver_matches
        order by league_code
        """
    )
    return data["league_code"].dropna().tolist()


@st.cache_data(ttl=300, show_spinner=False)
def available_seasons() -> list[str]:
    data = run_query(
        """
        select distinct season_label
        from semantic.silver_matches
        order by season_label desc
        """
    )
    return data["season_label"].dropna().tolist()


@st.cache_data(ttl=300, show_spinner=False)
def available_teams(league_code: str, season_label: str) -> list[str]:
    sql = f"""
        select distinct team_name
        from (
            select home_team as team_name
            from semantic.silver_matches
            where league_code = '{_quote(league_code)}'
              and season_label = '{_quote(season_label)}'
            union
            select away_team as team_name
            from semantic.silver_matches
            where league_code = '{_quote(league_code)}'
              and season_label = '{_quote(season_label)}'
        )
        order by team_name
    """
    data = run_query(sql)
    return data["team_name"].dropna().tolist()


@st.cache_data(ttl=300, show_spinner=False)
def load_match_context(league_code: str, season_label: str, limit: int = 200) -> pd.DataFrame:
    sql = f"""
        select
            match_date,
            home_team,
            away_team,
            result_ft,
            home_last5_points_pg,
            away_last5_points_pg,
            home_last5_goals_for_pg,
            away_last5_goals_for_pg,
            odds_b365_home,
            odds_b365_draw,
            odds_b365_away
        from semantic.gold_match_context
        where league_code = '{_quote(league_code)}'
          and season_label = '{_quote(season_label)}'
        order by match_date desc, home_team asc
        limit {limit}
    """
    return run_query(sql)


@st.cache_data(ttl=300, show_spinner=False)
def load_h2h_context(league_code: str, season_label: str, home_team: str, away_team: str) -> pd.DataFrame:
    sql = f"""
        select
            match_date,
            home_team,
            away_team,
            h2h_matches,
            last_h2h_match_date,
            h2h_home_win_rate,
            h2h_draw_rate,
            h2h_away_win_rate,
            h2h_goal_diff_pg,
            h2h_gap,
            odds_b365_home,
            odds_b365_draw,
            odds_b365_away
        from semantic.gold_h2h_context
        where league_code = '{_quote(league_code)}'
          and season_label = '{_quote(season_label)}'
          and home_team = '{_quote(home_team)}'
          and away_team = '{_quote(away_team)}'
        order by match_date desc
        limit 25
    """
    return run_query(sql)


@st.cache_data(ttl=300, show_spinner=False)
def load_recent_matches(league_code: str, home_team: str, away_team: str) -> pd.DataFrame:
    sql = f"""
        select
            match_date,
            season_label,
            home_team,
            away_team,
            home_goals_ft,
            away_goals_ft,
            result_ft
        from semantic.silver_matches
        where league_code = '{_quote(league_code)}'
          and (
            (home_team = '{_quote(home_team)}' and away_team = '{_quote(away_team)}')
            or
            (home_team = '{_quote(away_team)}' and away_team = '{_quote(home_team)}')
          )
        order by match_date desc
        limit 20
    """
    return run_query(sql)


@st.cache_data(ttl=300, show_spinner=False)
def load_standings(league_code: str, season_label: str) -> pd.DataFrame:
    sql = f"""
        select
            table_position,
            team_name,
            matches_played,
            wins,
            draws,
            losses,
            goals_for,
            goals_against,
            goal_diff,
            points
        from semantic.gold_standings
        where league_code = '{_quote(league_code)}'
          and season_label = '{_quote(season_label)}'
        order by table_position asc
    """
    return run_query(sql)


def render_overview() -> None:
    metrics = overview_metrics()
    summary = metrics["summary"]
    pipeline = metrics["pipeline"]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Matches Loaded", f"{int(summary.get('matches_loaded', 0)):,}")
    col2.metric("Leagues", int(summary.get("leagues_loaded", 0)))
    col3.metric("Seasons", int(summary.get("seasons_loaded", 0)))
    col4.metric("Latest Row Count", f"{int(pipeline.get('row_count', 0)):,}")

    st.caption(
        f"Latest successful pipeline run: `{pipeline.get('run_id', 'n/a')}` | "
        f"status: `{pipeline.get('status', 'n/a')}` | "
        f"completed_at: `{pipeline.get('completed_at', 'n/a')}`"
    )

    date_col1, date_col2 = st.columns(2)
    date_col1.metric("First Match Date", str(summary.get("first_match_date", "n/a")))
    date_col2.metric("Last Match Date", str(summary.get("last_match_date", "n/a")))


def render_match_context() -> None:
    leagues = available_leagues()
    seasons = available_seasons()

    col1, col2 = st.columns(2)
    selected_league = col1.selectbox("League", leagues, index=0 if leagues else None)
    selected_season = col2.selectbox("Season", seasons, index=0 if seasons else None)

    if not selected_league or not selected_season:
        st.info("No match context data available yet.")
        return

    data = load_match_context(selected_league, selected_season)
    st.dataframe(data, use_container_width=True, hide_index=True)


def render_h2h() -> None:
    leagues = available_leagues()
    seasons = available_seasons()

    col1, col2 = st.columns(2)
    selected_league = col1.selectbox("League", leagues, key="h2h_league")
    selected_season = col2.selectbox("Season", seasons, key="h2h_season")

    if not selected_league or not selected_season:
        st.info("No H2H data available yet.")
        return

    teams = available_teams(selected_league, selected_season)
    if len(teams) < 2:
        st.info("Not enough teams available for H2H analysis.")
        return

    team_col1, team_col2 = st.columns(2)
    home_team = team_col1.selectbox("Home Team", teams, key="h2h_home")
    default_away_index = 1 if len(teams) > 1 else 0
    away_team = team_col2.selectbox("Away Team", teams, index=default_away_index, key="h2h_away")

    if home_team == away_team:
        st.warning("Select two different teams to inspect head-to-head context.")
        return

    h2h_data = load_h2h_context(selected_league, selected_season, home_team, away_team)
    if h2h_data.empty:
        st.info("No H2H context is available for that fixture in the selected season.")
    else:
        latest = h2h_data.iloc[0]
        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
        metric_col1.metric("H2H Matches", int(latest["h2h_matches"]))
        metric_col2.metric("Home Win Rate", f"{float(latest['h2h_home_win_rate']):.1%}")
        metric_col3.metric("Draw Rate", f"{float(latest['h2h_draw_rate']):.1%}")
        metric_col4.metric("Away Win Rate", f"{float(latest['h2h_away_win_rate']):.1%}")
        st.metric("H2H Goal Diff Per Game", f"{float(latest['h2h_goal_diff_pg']):.2f}")
        st.dataframe(h2h_data, use_container_width=True, hide_index=True)

    st.subheader("Recent Meetings")
    recent_matches = load_recent_matches(selected_league, home_team, away_team)
    st.dataframe(recent_matches, use_container_width=True, hide_index=True)


def render_standings() -> None:
    leagues = available_leagues()
    seasons = available_seasons()

    col1, col2 = st.columns(2)
    selected_league = col1.selectbox("League", leagues, key="standings_league")
    selected_season = col2.selectbox("Season", seasons, key="standings_season")

    if not selected_league or not selected_season:
        st.info("No standings data available yet.")
        return

    standings = load_standings(selected_league, selected_season)
    st.dataframe(standings, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(
        page_title="Football Betting Data Platform",
        page_icon=":bar_chart:",
        layout="wide",
    )

    st.title("Football Betting Data Platform")
    st.caption(
        "Streamlit is now acting as the serving layer over Dremio semantic datasets. "
        "All heavy transformation lives in dbt."
    )

    st.sidebar.header("Platform State")
    st.sidebar.write(f"Dremio host: `{DREMIO_HOST}:{DREMIO_PORT}`")
    st.sidebar.write(f"Today: `{date.today().isoformat()}`")
    if st.sidebar.button("Refresh Cached Queries"):
        st.cache_data.clear()
        st.rerun()

    overview_tab, standings_tab, match_tab, h2h_tab = st.tabs(["Overview", "Standings", "Match Context", "H2H"])
    with overview_tab:
        render_overview()
    with standings_tab:
        render_standings()
    with match_tab:
        render_match_context()
    with h2h_tab:
        render_h2h()


if __name__ == "__main__":
    main()
