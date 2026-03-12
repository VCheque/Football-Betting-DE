select
    cast(nullif("player_name", '')  as varchar) as player_name,
    cast(nullif("team", '')         as varchar) as team,
    cast(nullif("league_code", '')  as varchar) as league_code,
    cast(nullif("injury_type", '')  as varchar) as injury_type,
    -- return_date is optional; null means return date unknown
    case
        when nullif("return_date", '') is not null
            then cast("return_date" as date)
        else null
    end                                          as return_date
from {{ source('semantic', 'raw_injuries') }}
