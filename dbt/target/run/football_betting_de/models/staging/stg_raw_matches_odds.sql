

  create or replace view "semantic"."stg_raw_matches_odds"
  
  as select
    to_date(nullif("Date", ''), 'DD/MM/YYYY') as match_date,
    cast(nullif("HomeTeam", '') as varchar) as home_team,
    cast(nullif("AwayTeam", '') as varchar) as away_team,
    cast(nullif("FTR", '') as varchar) as result_ft,
    cast(nullif("LeagueCode", '') as varchar) as league_code,
    cast(nullif("Season", '') as varchar) as season_label,
    cast(nullif("RunId", '') as varchar) as run_id,
    cast(nullif("IngestDate", '') as date) as ingest_date,
    cast(nullif("FTHG", '') as integer) as home_goals_ft,
    cast(nullif("FTAG", '') as integer) as away_goals_ft,
    cast(nullif("HTHG", '') as integer) as home_goals_ht,
    cast(nullif("HTAG", '') as integer) as away_goals_ht,
    cast(nullif("HS", '') as integer) as home_shots,
    cast(nullif("AS", '') as integer) as away_shots,
    cast(nullif("HST", '') as integer) as home_shots_on_target,
    cast(nullif("AST", '') as integer) as away_shots_on_target,
    cast(nullif("HF", '') as integer) as home_fouls,
    cast(nullif("AF", '') as integer) as away_fouls,
    cast(nullif("HC", '') as integer) as home_corners,
    cast(nullif("AC", '') as integer) as away_corners,
    cast(nullif("HY", '') as integer) as home_yellow_cards,
    cast(nullif("AY", '') as integer) as away_yellow_cards,
    cast(nullif("HR", '') as integer) as home_red_cards,
    cast(nullif("AR", '') as integer) as away_red_cards,
    cast(nullif("B365H", '') as double) as odds_b365_home,
    cast(nullif("B365D", '') as double) as odds_b365_draw,
    cast(nullif("B365A", '') as double) as odds_b365_away,
    cast(nullif("PSH", '') as double) as odds_pinnacle_home,
    cast(nullif("PSD", '') as double) as odds_pinnacle_draw,
    cast(nullif("PSA", '') as double) as odds_pinnacle_away,
    cast(nullif("AvgH", '') as double) as odds_avg_home,
    cast(nullif("AvgD", '') as double) as odds_avg_draw,
    cast(nullif("AvgA", '') as double) as odds_avg_away
from "semantic"."raw_matches_odds"