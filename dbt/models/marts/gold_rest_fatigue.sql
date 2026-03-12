-- Builds a list of all team appearances (home + away) with the match date,
-- then computes for each match: days since last appearance and match count
-- in the previous 21 days as a fixture-congestion / fatigue proxy.

with team_appearances as (
    -- Home appearances
    select
        match_date,
        league_code,
        season_label,
        home_team as team,
        'home'    as role
    from {{ ref('silver_matches') }}

    union all

    -- Away appearances
    select
        match_date,
        league_code,
        season_label,
        away_team as team,
        'away'    as role
    from {{ ref('silver_matches') }}
),

with_prev_match as (
    select
        match_date,
        league_code,
        season_label,
        team,
        role,
        -- Most recent prior match date for this team (any competition in the dataset)
        lag(match_date) over (
            partition by team
            order by match_date
        ) as prev_match_date
    from team_appearances
),

with_rest as (
    select
        match_date,
        league_code,
        season_label,
        team,
        role,
        prev_match_date,
        -- Days since last match; null when no prior match exists
        case
            when prev_match_date is not null
                then TIMESTAMPDIFF(DAY, prev_match_date, match_date)
            else null
        end as days_rest
    from with_prev_match
),

-- Count how many matches each team played in the 21 days before this match
-- (excluding the match itself) as a proxy for fixture congestion
fatigue_counts as (
    select
        a.match_date,
        a.league_code,
        a.season_label,
        a.team,
        a.role,
        a.days_rest,
        count(b.match_date) as matches_last_21d
    from with_rest a
    left join team_appearances b
        on  a.team = b.team
        and b.match_date < a.match_date
        and TIMESTAMPDIFF(DAY, b.match_date, a.match_date) <= 21
    group by
        a.match_date,
        a.league_code,
        a.season_label,
        a.team,
        a.role,
        a.days_rest
),

-- Pivot back to one row per match with home/away columns
home_stats as (
    select
        match_date,
        league_code,
        season_label,
        team          as home_team,
        days_rest     as home_days_rest,
        matches_last_21d as home_matches_last_21d
    from fatigue_counts
    where role = 'home'
),

away_stats as (
    select
        match_date,
        league_code,
        season_label,
        team          as away_team,
        days_rest     as away_days_rest,
        matches_last_21d as away_matches_last_21d
    from fatigue_counts
    where role = 'away'
)

select
    h.match_date,
    h.league_code,
    h.season_label,
    h.home_team,
    a.away_team,
    h.home_days_rest,
    a.away_days_rest,
    -- rest_gap > 0 means home team is more rested
    coalesce(h.home_days_rest, 7) - coalesce(a.away_days_rest, 7) as rest_gap,
    h.home_matches_last_21d,
    a.away_matches_last_21d,
    -- fatigue_gap > 0 means away team has played more recently (more fatigued)
    a.away_matches_last_21d - h.home_matches_last_21d               as fatigue_gap
from home_stats h
inner join away_stats a
    on  h.match_date   = a.match_date
    and h.league_code  = a.league_code
    and h.season_label = a.season_label
