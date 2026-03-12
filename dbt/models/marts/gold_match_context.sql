-- gold_match_context.sql
-- Pre-computes per-match rolling context features for each team so that
-- the Streamlit app and ML models can read directly from Dremio without
-- recomputing window functions in Python.
--
-- Window convention:  ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
--   = last 5 completed matches before the current one (no data leakage).
--
-- v2 additions:
--   * home/away_last5_sot_diff_pg  — shots-on-target differential (proxy for xG)
--   * home/away_momentum_slope     — simplified trend: recent 2-game avg minus
--                                    older 3-game avg, divided by 3 (max pts/game).
--                                    Positive = improving; negative = declining.

with base as (
    select
        match_date,
        league_code,
        season_label,
        home_team,
        away_team,
        home_goals_ft,
        away_goals_ft,
        result_ft,
        home_corners,
        away_corners,
        home_yellow_cards,
        away_yellow_cards,
        home_shots,
        away_shots,
        home_shots_on_target,
        away_shots_on_target,
        odds_b365_home,
        odds_b365_draw,
        odds_b365_away,
        -- ── Home/away perspective points (needed for momentum windows) ────
        case result_ft when 'H' then 3 when 'D' then 1 else 0 end as home_pts,
        case result_ft when 'A' then 3 when 'D' then 1 else 0 end as away_pts
    from {{ ref('silver_matches') }}
)

select
    match_date,
    league_code,
    season_label,
    home_team,
    away_team,
    home_goals_ft,
    away_goals_ft,
    result_ft,
    odds_b365_home,
    odds_b365_draw,
    odds_b365_away,

    -- ── Rolling form: points per game (last 5 matches) ───────────────────
    avg(home_pts) over (
        partition by league_code, home_team
        order by match_date
        rows between 5 preceding and 1 preceding
    ) as home_last5_points_pg,
    avg(away_pts) over (
        partition by league_code, away_team
        order by match_date
        rows between 5 preceding and 1 preceding
    ) as away_last5_points_pg,

    -- ── Rolling attack / defence: goals per game (last 5 matches) ────────
    avg(home_goals_ft) over (
        partition by league_code, home_team
        order by match_date
        rows between 5 preceding and 1 preceding
    ) as home_last5_goals_for_pg,
    avg(away_goals_ft) over (
        partition by league_code, home_team
        order by match_date
        rows between 5 preceding and 1 preceding
    ) as home_last5_goals_against_pg,
    avg(away_goals_ft) over (
        partition by league_code, away_team
        order by match_date
        rows between 5 preceding and 1 preceding
    ) as away_last5_goals_for_pg,
    avg(home_goals_ft) over (
        partition by league_code, away_team
        order by match_date
        rows between 5 preceding and 1 preceding
    ) as away_last5_goals_against_pg,

    -- ── Rolling corners: total corners per game (last 5 matches) ─────────
    avg(home_corners + away_corners) over (
        partition by league_code, home_team
        order by match_date
        rows between 5 preceding and 1 preceding
    ) as home_last5_corners_pg,
    avg(home_corners + away_corners) over (
        partition by league_code, away_team
        order by match_date
        rows between 5 preceding and 1 preceding
    ) as away_last5_corners_pg,

    -- ── Rolling discipline: yellow cards per game (last 5 matches) ───────
    avg(home_yellow_cards) over (
        partition by league_code, home_team
        order by match_date
        rows between 5 preceding and 1 preceding
    ) as home_last5_cards_pg,
    avg(away_yellow_cards) over (
        partition by league_code, away_team
        order by match_date
        rows between 5 preceding and 1 preceding
    ) as away_last5_cards_pg,

    -- ── Rolling shots: total shots per game (last 5 matches) ─────────────
    avg(home_shots) over (
        partition by league_code, home_team
        order by match_date
        rows between 5 preceding and 1 preceding
    ) as home_last5_shots_pg,
    avg(away_shots) over (
        partition by league_code, away_team
        order by match_date
        rows between 5 preceding and 1 preceding
    ) as away_last5_shots_pg,

    -- ── NEW v2: Shots-on-target differential per game (proxy for xG) ─────
    -- Positive = team creates more quality chances than it concedes per game.
    avg(home_shots_on_target - away_shots_on_target) over (
        partition by league_code, home_team
        order by match_date
        rows between 5 preceding and 1 preceding
    ) as home_last5_sot_diff_pg,
    avg(away_shots_on_target - home_shots_on_target) over (
        partition by league_code, away_team
        order by match_date
        rows between 5 preceding and 1 preceding
    ) as away_last5_sot_diff_pg,

    -- ── NEW v2: Momentum slope (simplified OLS approximation) ────────────
    -- avg(recent 2 games pts) − avg(older 3 games pts), normalised by 3.
    -- Positive = improving form; negative = declining.
    -- Consistent with the Python OLS slope used in model training.
    (
        avg(home_pts) over (
            partition by league_code, home_team
            order by match_date
            rows between 2 preceding and 1 preceding
        )
        -
        avg(home_pts) over (
            partition by league_code, home_team
            order by match_date
            rows between 5 preceding and 3 preceding
        )
    ) / 3.0 as home_momentum_slope,

    (
        avg(away_pts) over (
            partition by league_code, away_team
            order by match_date
            rows between 2 preceding and 1 preceding
        )
        -
        avg(away_pts) over (
            partition by league_code, away_team
            order by match_date
            rows between 5 preceding and 3 preceding
        )
    ) / 3.0 as away_momentum_slope

from base
