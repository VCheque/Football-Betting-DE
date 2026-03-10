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
    avg(
        case result_ft
            when 'H' then 3
            when 'D' then 1
            else 0
        end
    ) over (
        partition by league_code, home_team
        order by match_date
        rows between 5 preceding and 1 preceding
    ) as home_last5_points_pg,
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
    avg(
        case result_ft
            when 'A' then 3
            when 'D' then 1
            else 0
        end
    ) over (
        partition by league_code, away_team
        order by match_date
        rows between 5 preceding and 1 preceding
    ) as away_last5_points_pg,
    avg(away_goals_ft) over (
        partition by league_code, away_team
        order by match_date
        rows between 5 preceding and 1 preceding
    ) as away_last5_goals_for_pg,
    avg(home_goals_ft) over (
        partition by league_code, away_team
        order by match_date
        rows between 5 preceding and 1 preceding
    ) as away_last5_goals_against_pg
from "semantic"."silver_matches"