select
    cast(nullif("match_date", '')   as date)    as match_date,
    cast(nullif("league_code", '')  as varchar)  as league_code,
    cast(nullif("home_team", '')    as varchar)  as home_team,
    cast(nullif("away_team", '')    as varchar)  as away_team,
    cast(nullif("result_ft", '')    as varchar)  as result_ft,
    cast(nullif("espn_status", '')  as varchar)  as espn_status
from {{ source('semantic', 'raw_upcoming_fixtures') }}
where nullif("match_date", '') is not null
