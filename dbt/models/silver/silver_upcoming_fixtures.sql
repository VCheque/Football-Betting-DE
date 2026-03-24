{{ config(materialized='table') }}

with deduped as (
    select
        match_date,
        league_code,
        home_team,
        away_team,
        result_ft,
        espn_status,
        row_number() over (
            partition by league_code, match_date, home_team, away_team
            order by match_date desc
        ) as record_priority
    from {{ ref('stg_upcoming_fixtures') }}
    where match_date is not null
      and home_team is not null
      and away_team is not null
)

select
    match_date,
    league_code,
    case league_code
        when 'E0'  then 'Premier League'
        when 'SP1' then 'La Liga'
        when 'I1'  then 'Serie A'
        when 'D1'  then 'Bundesliga'
        when 'F1'  then 'Ligue 1'
        when 'P1'  then 'Primeira Liga'
        else league_code
    end as league_name,
    home_team,
    away_team,
    result_ft,
    espn_status
from deduped
where record_priority = 1
