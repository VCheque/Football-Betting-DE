-- Per-player, per-team, per-season aggregated statistics sourced from Understat.
-- Covers five leagues: Premier League, La Liga, Serie A, Bundesliga, Ligue 1.
-- Primeira Liga is excluded — not available on Understat.
--
-- Derived rate columns (per-90 basis) are used as lambda inputs for the
-- Poisson-based player scoring / card probability model in the Streamlit app.

select
    player_id,
    player_name,
    team,
    player_position,
    league_code,
    season_label,
    games,
    minutes_played,
    goals,
    assists,
    shots,
    key_passes,
    yellow_cards,
    red_cards,
    npg,
    xg,
    xa,
    npxg,

    -- Goals per 90 minutes — Poisson lambda for scoring probability
    cast(goals as double) / nullif(cast(minutes_played as double) / 90.0, 0)
        as goals_per_90,

    -- Assists per 90 minutes
    cast(assists as double) / nullif(cast(minutes_played as double) / 90.0, 0)
        as assists_per_90,

    -- Expected goals per 90 (non-penalty, more stable than raw goals)
    cast(npxg as double) / nullif(cast(minutes_played as double) / 90.0, 0)
        as npxg_per_90,

    -- Expected assists per 90
    cast(xa as double) / nullif(cast(minutes_played as double) / 90.0, 0)
        as xa_per_90,

    -- Yellow cards per 90 — Poisson lambda for card probability
    cast(yellow_cards as double) / nullif(cast(minutes_played as double) / 90.0, 0)
        as yellow_cards_per_90,

    -- xG per game (total xg across all matches / games played)
    cast(xg as double) / nullif(cast(games as double), 0)
        as xg_per_game,

    -- Score contribution weight (goals + 0.4 * assists) per 90 for lineup_strength
    (cast(goals as double) + 0.4 * cast(assists as double))
        / nullif(cast(minutes_played as double) / 90.0, 0)
        as score_contrib_per_90

from {{ ref('stg_player_stats') }}
where player_name is not null
  and team is not null
  and league_code is not null
  and season_label is not null
