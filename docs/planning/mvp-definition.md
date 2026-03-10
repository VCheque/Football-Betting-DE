# MVP Definition

## MVP Name

**Match Results and Odds Pipeline**

## Objective

Build the first end-to-end data product for the football betting platform using one reliable source and one clear business output.

This MVP should prove that the platform can:

1. ingest raw football data,
2. store it in a raw data layer,
3. track ingestion metadata,
4. transform raw data into a trusted analytical dataset,
5. expose a curated dataset for downstream analytics and app usage.

## Business Goal

Create a trusted match-level dataset that can support:

- league standings,
- team form analysis,
- head-to-head analysis,
- betting feature engineering,
- future prediction workflows.

## Source

**Primary source:** `football-data.co.uk`

## First Entity

**Entity name:** `matches_odds`

This entity contains historical football matches with match outcomes and bookmaker odds.

## Why This MVP First

This is the best first slice because it is the core dataset behind most of the existing football application logic.

It supports:

- standings,
- match history,
- form metrics,
- H2H metrics,
- betting features.

It is also simpler than starting with injuries or player-level data.

## Grain

**One row per match**

A row represents one football match between one home team and one away team on one date in one competition.

## End-to-End Scope

This MVP includes:

- raw ingestion into Bronze
- metadata tracking in PostgreSQL
- Silver transformation in dbt
- Gold transformation in dbt
- semantic access through Dremio

This MVP does **not** yet include:

- player stats
- injuries
- model training
- Streamlit dashboards
- multi-source entity reconciliation beyond basic team normalization

## Data Flow

1. Download raw season CSV files from `football-data.co.uk`
2. Store original files in MinIO Bronze
3. Register ingestion run metadata in PostgreSQL
4. Expose Bronze + PostgreSQL metadata through Dremio
5. Build Silver `matches` model in dbt
6. Build Gold `match_context` or `match_features` model in dbt

## Bronze Layer Definition

### Purpose

Store the raw source data exactly as received.

### Storage

**System:** MinIO

### Format

Original source CSV files

### Path Pattern

```text
s3://football/bronze/source=football_data_co_uk/entity=matches_odds/ingest_date=YYYY-MM-DD/run_id=<uuid>/league=<code>/season=<yyyy>/file.csv
```

### Bronze Contents

Each file should remain as close as possible to the source version.

Example contents:

- raw match date
- home team
- away team
- full-time result
- goals
- odds columns
- league code
- season data

### Bronze Rules

- immutable
- versioned by run
- no business transformation
- preserve original file structure as much as possible

## PostgreSQL Metadata Scope

### Purpose

Track runs, files, and reference data.

### Tables Needed for MVP

#### `pipeline_run`

Tracks each ingestion run.

Fields:

- `run_id`
- `source_name`
- `entity_name`
- `status`
- `started_at`
- `completed_at`
- `row_count`
- `checksum`
- `error_message`

#### `file_manifest`

Tracks each ingested file.

Fields:

- `run_id`
- `bucket_name`
- `object_key`
- `file_name`
- `source_url`
- `checksum`
- `byte_size`
- `row_count`

#### `source_config`

Tracks source-level configuration.

Fields:

- `source_name`
- `entity_name`
- `is_enabled`
- `cadence`
- `config_json`

#### `dim_league`

Stores canonical league definitions.

Fields:

- `league_id`
- `source_league_key`
- `league_name`
- `country_name`

#### `dim_team`

Stores canonical team definitions.

Fields:

- `team_id`
- `source_team_key`
- `team_name`

#### `team_alias_map`

Maps source team names to canonical names.

Fields:

- `alias_id`
- `team_id`
- `source_name`
- `alias_name`

## Silver Model Definition

### Model Name

`silver_matches`

### Purpose

Create a clean, typed, standardized match table from the raw source files.

### Silver Grain

One row per match

### Silver Responsibilities

- rename columns into a clean convention
- cast dates and numeric fields correctly
- standardize team names
- standardize league identifiers
- remove invalid rows
- deduplicate records if necessary

### Expected Silver Columns

- `match_date`
- `league_code`
- `league_name`
- `season_start`
- `season_label`
- `home_team`
- `away_team`
- `home_goals_ft`
- `away_goals_ft`
- `home_goals_ht`
- `away_goals_ht`
- `result_ft`
- `home_shots`
- `away_shots`
- `home_shots_on_target`
- `away_shots_on_target`
- `home_fouls`
- `away_fouls`
- `home_corners`
- `away_corners`
- `home_yellow_cards`
- `away_yellow_cards`
- `home_red_cards`
- `away_red_cards`
- `odds_b365_home`
- `odds_b365_draw`
- `odds_b365_away`
- `odds_pinnacle_home`
- `odds_pinnacle_draw`
- `odds_pinnacle_away`
- `odds_avg_home`
- `odds_avg_draw`
- `odds_avg_away`
- `source_url`
- `run_id`

## Gold Model Definition

### Model Name

`gold_match_context`

### Purpose

Create a business-ready dataset for match analysis and downstream betting features.

### Gold Grain

One row per match

### Gold Responsibilities

- derive team form metrics
- calculate simple standings context
- calculate goal and points trends
- prepare stable columns for downstream analytics and app consumption

### Example Gold Columns

- `match_date`
- `league_name`
- `season_label`
- `home_team`
- `away_team`
- `home_position`
- `away_position`
- `home_points`
- `away_points`
- `home_last5_points_pg`
- `away_last5_points_pg`
- `home_last5_goals_for_pg`
- `away_last5_goals_for_pg`
- `home_last5_goals_against_pg`
- `away_last5_goals_against_pg`
- `home_win_rate`
- `away_win_rate`
- `odds_b365_home`
- `odds_b365_draw`
- `odds_b365_away`

## Data Quality Checks

### Ingestion Checks

- source file downloaded successfully
- file is not empty
- checksum is generated
- row count is captured
- run status is recorded

### Silver Checks

- `match_date` is not null
- `home_team` is not null
- `away_team` is not null
- `league_code` is not null
- `result_ft` is in accepted values such as `H`, `D`, `A`
- no duplicate match rows for the same date, teams, and league

### Gold Checks

- one row per match
- standings/form metrics are populated where historical data exists
- odds fields are numeric where available

## Success Criteria

This MVP is successful when:

1. raw match data is ingested into Bronze,
2. ingestion metadata is stored in PostgreSQL,
3. Dremio can query the raw and metadata layers,
4. dbt builds `silver_matches`,
5. dbt builds `gold_match_context`,
6. the final dataset is understandable and reusable for app or analytics use cases.

## Interview Summary

This MVP demonstrates:

- object storage as the raw data layer
- metadata-driven ingestion
- separation of raw, clean, and curated datasets
- dbt-based transformation and testing
- semantic access through Dremio
- a realistic first slice of a modern data platform
