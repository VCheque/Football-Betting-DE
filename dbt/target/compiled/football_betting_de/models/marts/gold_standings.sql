with team_results as (
    select
        match_date,
        league_code,
        season_label,
        home_team as team_name,
        away_team as opponent_team,
        'home' as venue,
        home_goals_ft as goals_for,
        away_goals_ft as goals_against,
        case
            when result_ft = 'H' then 3
            when result_ft = 'D' then 1
            else 0
        end as points_earned,
        case when result_ft = 'H' then 1 else 0 end as wins_earned,
        case when result_ft = 'D' then 1 else 0 end as draws_earned,
        case when result_ft = 'A' then 1 else 0 end as losses_earned
    from "semantic"."silver_matches"
    where result_ft in ('H', 'D', 'A')

    union all

    select
        match_date,
        league_code,
        season_label,
        away_team as team_name,
        home_team as opponent_team,
        'away' as venue,
        away_goals_ft as goals_for,
        home_goals_ft as goals_against,
        case
            when result_ft = 'A' then 3
            when result_ft = 'D' then 1
            else 0
        end as points_earned,
        case when result_ft = 'A' then 1 else 0 end as wins_earned,
        case when result_ft = 'D' then 1 else 0 end as draws_earned,
        case when result_ft = 'H' then 1 else 0 end as losses_earned
    from "semantic"."silver_matches"
    where result_ft in ('H', 'D', 'A')
),

rolling_totals as (
    select
        match_date,
        league_code,
        season_label,
        team_name,
        opponent_team,
        venue,
        count(*) over (
            partition by league_code, season_label, team_name
            order by match_date
            rows between unbounded preceding and current row
        ) as matches_played,
        sum(wins_earned) over (
            partition by league_code, season_label, team_name
            order by match_date
            rows between unbounded preceding and current row
        ) as wins,
        sum(draws_earned) over (
            partition by league_code, season_label, team_name
            order by match_date
            rows between unbounded preceding and current row
        ) as draws,
        sum(losses_earned) over (
            partition by league_code, season_label, team_name
            order by match_date
            rows between unbounded preceding and current row
        ) as losses,
        sum(goals_for) over (
            partition by league_code, season_label, team_name
            order by match_date
            rows between unbounded preceding and current row
        ) as goals_for,
        sum(goals_against) over (
            partition by league_code, season_label, team_name
            order by match_date
            rows between unbounded preceding and current row
        ) as goals_against,
        sum(points_earned) over (
            partition by league_code, season_label, team_name
            order by match_date
            rows between unbounded preceding and current row
        ) as points,
        row_number() over (
            partition by league_code, season_label, team_name
            order by match_date desc, opponent_team desc
        ) as team_recency_rank
    from team_results
),

latest_team_state as (
    select
        match_date as last_match_date,
        league_code,
        season_label,
        team_name,
        matches_played,
        wins,
        draws,
        losses,
        goals_for,
        goals_against,
        goals_for - goals_against as goal_diff,
        points
    from rolling_totals
    where team_recency_rank = 1
),

ranked_standings as (
    select
        last_match_date,
        league_code,
        season_label,
        team_name,
        matches_played,
        wins,
        draws,
        losses,
        goals_for,
        goals_against,
        goal_diff,
        points,
        row_number() over (
            partition by league_code, season_label
            order by points desc, goal_diff desc, goals_for desc, team_name asc
        ) as table_position
    from latest_team_state
)

select
    concat(season_label, '|', league_code, '|', team_name) as standing_key,
    last_match_date,
    league_code,
    season_label,
    team_name,
    table_position,
    matches_played,
    wins,
    draws,
    losses,
    goals_for,
    goals_against,
    goal_diff,
    points
from ranked_standings