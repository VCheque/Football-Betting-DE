with base_matches as (
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
        odds_b365_away
    from {{ ref('silver_matches') }}
    where result_ft in ('H', 'D', 'A')
),

historical_matchups as (
    select
        current_matches.match_date,
        current_matches.league_code,
        current_matches.season_label,
        current_matches.home_team,
        current_matches.away_team,
        current_matches.home_goals_ft,
        current_matches.away_goals_ft,
        current_matches.result_ft,
        current_matches.odds_b365_home,
        current_matches.odds_b365_draw,
        current_matches.odds_b365_away,
        historical_matches.match_date as prior_match_date,
        case
            when historical_matches.home_team = current_matches.home_team
             and historical_matches.away_team = current_matches.away_team
                then historical_matches.result_ft
            when historical_matches.result_ft = 'H'
                then 'A'
            when historical_matches.result_ft = 'A'
                then 'H'
            else 'D'
        end as perspective_result,
        case
            when historical_matches.home_team = current_matches.home_team
             and historical_matches.away_team = current_matches.away_team
                then historical_matches.home_goals_ft - historical_matches.away_goals_ft
            else historical_matches.away_goals_ft - historical_matches.home_goals_ft
        end as perspective_goal_diff,
        row_number() over (
            partition by
                current_matches.match_date,
                current_matches.league_code,
                current_matches.home_team,
                current_matches.away_team
            order by historical_matches.match_date desc
        ) as h2h_recency_rank
    from base_matches current_matches
    left join base_matches historical_matches
        on current_matches.league_code = historical_matches.league_code
       and historical_matches.match_date < current_matches.match_date
       and (
            (
                historical_matches.home_team = current_matches.home_team
                and historical_matches.away_team = current_matches.away_team
            )
            or (
                historical_matches.home_team = current_matches.away_team
                and historical_matches.away_team = current_matches.home_team
            )
       )
),

weighted_matchups as (
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
        prior_match_date,
        perspective_result,
        perspective_goal_diff,
        power(0.5, cast(h2h_recency_rank - 1 as double) / 3.0) as h2h_weight
    from historical_matchups
    where prior_match_date is not null
),

aggregated_h2h as (
    select
        base_matches.match_date,
        base_matches.league_code,
        base_matches.season_label,
        base_matches.home_team,
        base_matches.away_team,
        base_matches.home_goals_ft,
        base_matches.away_goals_ft,
        base_matches.result_ft,
        base_matches.odds_b365_home,
        base_matches.odds_b365_draw,
        base_matches.odds_b365_away,
        count(weighted_matchups.prior_match_date) as h2h_matches,
        max(weighted_matchups.prior_match_date) as last_h2h_match_date,
        sum(weighted_matchups.h2h_weight) as total_h2h_weight,
        sum(
            case
                when weighted_matchups.perspective_result = 'H'
                    then weighted_matchups.h2h_weight
                else 0.0
            end
        ) as home_win_weight,
        sum(
            case
                when weighted_matchups.perspective_result = 'D'
                    then weighted_matchups.h2h_weight
                else 0.0
            end
        ) as draw_weight,
        sum(
            case
                when weighted_matchups.perspective_result = 'A'
                    then weighted_matchups.h2h_weight
                else 0.0
            end
        ) as away_win_weight,
        sum(weighted_matchups.perspective_goal_diff * weighted_matchups.h2h_weight) as goal_diff_weight
    from base_matches
    left join weighted_matchups
        on base_matches.match_date = weighted_matchups.match_date
       and base_matches.league_code = weighted_matchups.league_code
       and base_matches.home_team = weighted_matchups.home_team
       and base_matches.away_team = weighted_matchups.away_team
    group by
        base_matches.match_date,
        base_matches.league_code,
        base_matches.season_label,
        base_matches.home_team,
        base_matches.away_team,
        base_matches.home_goals_ft,
        base_matches.away_goals_ft,
        base_matches.result_ft,
        base_matches.odds_b365_home,
        base_matches.odds_b365_draw,
        base_matches.odds_b365_away
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
    h2h_matches,
    last_h2h_match_date,
    coalesce(
        case
            when total_h2h_weight > 0
                then home_win_weight / total_h2h_weight
            else 0.0
        end,
        0.0
    ) as h2h_home_win_rate,
    coalesce(
        case
            when total_h2h_weight > 0
                then draw_weight / total_h2h_weight
            else 0.0
        end,
        0.0
    ) as h2h_draw_rate,
    coalesce(
        case
            when total_h2h_weight > 0
                then away_win_weight / total_h2h_weight
            else 0.0
        end,
        0.0
    ) as h2h_away_win_rate,
    coalesce(
        case
            when total_h2h_weight > 0
                then goal_diff_weight / total_h2h_weight
            else 0.0
        end,
        0.0
    ) as h2h_goal_diff_pg,
    coalesce(
        case
            when total_h2h_weight > 0
                then (home_win_weight - away_win_weight) / total_h2h_weight
            else 0.0
        end,
        0.0
    ) as h2h_gap
from aggregated_h2h
