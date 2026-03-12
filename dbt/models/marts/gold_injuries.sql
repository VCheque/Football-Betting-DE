-- Active injury records for use in the Streamlit app.
-- Sourced from a manually maintained CSV uploaded via ingestion/src/upload_injuries.py.
-- Used to compute the injury_gap ML feature and to render the "Player Intelligence"
-- section in Match Center (Tab 1) of the app.

select
    player_name,
    team,
    league_code,
    injury_type,
    return_date,
    -- Flag injuries with no known return date as indefinite
    case when return_date is null then true else false end as indefinite_return
from {{ ref('stg_injuries') }}
where player_name is not null
  and team is not null
