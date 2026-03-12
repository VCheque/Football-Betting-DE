-- Team statistics per season, broken down by scope:
--   all           → all matches in the season
--   home          → matches played at home
--   away          → matches played away
--   vs_top_half   → matches against opponents in the top half of the table
--   vs_bottom_half → matches against opponents in the bottom half of the table
--
-- Opponent tier is determined using the FINAL season standings (gold_standings),
-- where position <= half the team count = top half.

with standings as (
    select
        league_code,
        season_label,
        team,
        league_position,
        -- Determine whether a team is in the top half of the table for this season
        max(league_position) over (partition by league_code, season_label) as total_teams
    from {{ ref('gold_standings') }}
),

-- Flatten each match into two rows: one per team, tagging home/away role
team_matches as (
    -- Home team row
    select
        m.match_date,
        m.league_code,
        m.season_label,
        m.home_team                                                      as team,
        m.away_team                                                      as opponent,
        'home'                                                           as venue,
        m.home_goals_ft                                                  as goals_for,
        m.away_goals_ft                                                  as goals_against,
        case m.result_ft when 'H' then 3 when 'D' then 1 else 0 end     as points,
        case m.result_ft when 'H' then 1 else 0 end                      as wins,
        case m.result_ft when 'D' then 1 else 0 end                      as draws,
        case m.result_ft when 'A' then 1 else 0 end                      as losses,
        m.home_corners + m.away_corners                                  as total_corners,
        m.home_fouls + m.away_fouls                                      as total_fouls,
        m.home_yellow_cards + m.away_yellow_cards                        as total_yellow_cards
    from {{ ref('silver_matches') }} m
    where m.result_ft in ('H', 'D', 'A')

    union all

    -- Away team row
    select
        m.match_date,
        m.league_code,
        m.season_label,
        m.away_team                                                      as team,
        m.home_team                                                      as opponent,
        'away'                                                           as venue,
        m.away_goals_ft                                                  as goals_for,
        m.home_goals_ft                                                  as goals_against,
        case m.result_ft when 'A' then 3 when 'D' then 1 else 0 end     as points,
        case m.result_ft when 'A' then 1 else 0 end                      as wins,
        case m.result_ft when 'D' then 1 else 0 end                      as draws,
        case m.result_ft when 'H' then 1 else 0 end                      as losses,
        m.home_corners + m.away_corners                                  as total_corners,
        m.home_fouls + m.away_fouls                                      as total_fouls,
        m.home_yellow_cards + m.away_yellow_cards                        as total_yellow_cards
    from {{ ref('silver_matches') }} m
    where m.result_ft in ('H', 'D', 'A')
),

-- Attach opponent standing position to determine top/bottom half
enriched as (
    select
        tm.*,
        s_opp.league_position                                   as opponent_position,
        s_opp.total_teams                                       as league_size,
        case
            when s_opp.league_position <= s_opp.total_teams / 2 then 'vs_top_half'
            else 'vs_bottom_half'
        end                                                     as opponent_tier
    from team_matches tm
    left join standings s_opp
        on  tm.league_code  = s_opp.league_code
        and tm.season_label = s_opp.season_label
        and tm.opponent     = s_opp.team
),

-- Assign a match_scope label for each row — we will UNION four filters
scoped as (
    -- all matches
    select 'all' as match_scope, * from enriched

    union all

    -- home matches only
    select 'home' as match_scope, * from enriched where venue = 'home'

    union all

    -- away matches only
    select 'away' as match_scope, * from enriched where venue = 'away'

    union all

    -- vs top-half opponents
    select 'vs_top_half' as match_scope, * from enriched where opponent_tier = 'vs_top_half'

    union all

    -- vs bottom-half opponents
    select 'vs_bottom_half' as match_scope, * from enriched where opponent_tier = 'vs_bottom_half'
)

select
    league_code,
    season_label,
    team,
    match_scope,
    count(*)                                                   as matches_played,
    sum(wins)                                                  as wins,
    sum(draws)                                                 as draws,
    sum(losses)                                                as losses,
    sum(goals_for)                                             as goals_for,
    sum(goals_against)                                         as goals_against,
    sum(points)                                                as points,
    cast(sum(points) as double) / nullif(count(*), 0)         as ppg,
    cast(sum(total_corners) as double) / nullif(count(*), 0)  as corners_pg,
    cast(sum(total_fouls) as double) / nullif(count(*), 0)    as fouls_pg,
    cast(sum(total_yellow_cards) as double) / nullif(count(*), 0) as cards_pg,
    cast(sum(goals_for) as double) / nullif(count(*), 0)      as goals_for_pg,
    cast(sum(goals_against) as double) / nullif(count(*), 0)  as goals_against_pg
from scoped
group by league_code, season_label, team, match_scope
