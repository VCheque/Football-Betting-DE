

  create or replace view "semantic"."silver_matches"
  
  as select
    match_date,
    home_team,
    away_team,
    result_ft,
    league_code,
    season_label,
    run_id,
    ingest_date,
    home_goals_ft,
    away_goals_ft,
    home_goals_ht,
    away_goals_ht,
    home_shots,
    away_shots,
    home_shots_on_target,
    away_shots_on_target,
    home_fouls,
    away_fouls,
    home_corners,
    away_corners,
    home_yellow_cards,
    away_yellow_cards,
    home_red_cards,
    away_red_cards,
    odds_b365_home,
    odds_b365_draw,
    odds_b365_away,
    odds_pinnacle_home,
    odds_pinnacle_draw,
    odds_pinnacle_away,
    odds_avg_home,
    odds_avg_draw,
    odds_avg_away
from "$scratch"."silver_matches_physical"