with team_matches as (
    -- Home perspective: credit wins/draws/losses and goals to the home team
    select
        match_date,
        league_code,
        season_label,
        home_team                                                        as team,
        home_goals_ft                                                    as goals_for,
        away_goals_ft                                                    as goals_against,
        case result_ft when 'H' then 3 when 'D' then 1 else 0 end       as points,
        case result_ft when 'H' then 1 else 0 end                        as wins,
        case result_ft when 'D' then 1 else 0 end                        as draws,
        case result_ft when 'A' then 1 else 0 end                        as losses
    from {{ ref('silver_matches') }}
    where result_ft in ('H', 'D', 'A')

    union all

    -- Away perspective: credit wins/draws/losses and goals to the away team
    select
        match_date,
        league_code,
        season_label,
        away_team                                                        as team,
        away_goals_ft                                                    as goals_for,
        home_goals_ft                                                    as goals_against,
        case result_ft when 'A' then 3 when 'D' then 1 else 0 end       as points,
        case result_ft when 'A' then 1 else 0 end                        as wins,
        case result_ft when 'D' then 1 else 0 end                        as draws,
        case result_ft when 'H' then 1 else 0 end                        as losses
    from {{ ref('silver_matches') }}
    where result_ft in ('H', 'D', 'A')
),

aggregated as (
    select
        league_code,
        season_label,
        team,
        count(*)                                               as matches_played,
        sum(wins)                                              as wins,
        sum(draws)                                             as draws,
        sum(losses)                                            as losses,
        sum(goals_for)                                         as goals_for,
        sum(goals_against)                                     as goals_against,
        sum(goals_for) - sum(goals_against)                    as goal_diff,
        sum(points)                                            as points,
        cast(sum(points) as double) / nullif(count(*), 0)     as ppg
    from team_matches
    group by league_code, season_label, team
)

select
    league_code,
    season_label,
    team,
    matches_played,
    wins,
    draws,
    losses,
    goals_for,
    goals_against,
    goal_diff,
    points,
    ppg,
    rank() over (
        partition by league_code, season_label
        order by points desc, goal_diff desc, goals_for desc
    ) as league_position
from aggregated
