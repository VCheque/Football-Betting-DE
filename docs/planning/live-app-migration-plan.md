# Live App Migration Plan

## Objective

Use the data engineering platform in this repository as the foundation for replacing the current live Streamlit app.

The target is not only to reproduce the old app UI. The target is to move the product onto a cleaner architecture where:

- Python owns ingestion and orchestration
- PostgreSQL owns control-plane metadata and dimensions
- Dremio owns semantic access
- dbt owns Silver and Gold transformations
- Streamlit owns only the serving and interaction layer

## Current Source of Truth

Current live-app logic reference:

- [`/Users/valtercheque/Documents/Portfolio/Football Bets/app.py`](/Users/valtercheque/Documents/Portfolio/Football Bets/app.py)

Current platform serving app:

- [`/Users/valtercheque/Documents/Portfolio/Football-Betting-DE/streamlit/app.py`](/Users/valtercheque/Documents/Portfolio/Football-Betting-DE/streamlit/app.py)

Current live deployment planned for replacement:

- [gestao-tickets.streamlit.app](https://gestao-tickets.streamlit.app/)

## Migration Goal

Replace the old app with a new Streamlit app backed by the DE platform while preserving the user-facing capabilities that matter most:

- match analysis
- standings
- head-to-head analysis
- upcoming fixtures
- prediction support
- player intelligence
- bet builder / ticket generation

## Architecture Principle

The old app mixes:

- local file loading
- feature engineering
- model training
- external API calls
- UI rendering

The new app should separate those concerns:

1. ingestion jobs fetch and land data
2. metadata is tracked in PostgreSQL
3. dbt builds reusable Silver and Gold datasets
4. Dremio exposes stable semantic datasets
5. Streamlit consumes curated datasets and handles user interaction only

## Current State

Already implemented in the DE project:

- Bronze ingestion for match results and odds
- PostgreSQL control-plane metadata
- scheduled refresh 4x per day
- Dremio semantic layer
- dbt-owned `silver_matches`
- Gold marts:
  - `gold_match_context`
  - `gold_h2h_context`
  - `gold_standings`
- thin Streamlit app that reads semantic datasets

Still missing for live-app parity:

- player data pipeline
- injuries / player contributions pipeline
- upcoming fixtures semantic dataset
- prediction and recommendation layer
- full Match Center workflow
- full Bet Builder workflow
- PDF export
- multilingual UX parity
- refresh/status experience similar to the current app

## Feature Parity Matrix

### Already Covered

| Feature | Old app | New DE app | Status |
|---|---|---|---|
| Overview metrics | Yes | Yes | Implemented |
| Standings | Yes | Yes | Implemented |
| H2H context | Yes | Yes | Implemented |
| Match context browsing | Yes | Yes | Implemented |

### Partially Covered

| Feature | Gap |
|---|---|
| Match Center | New app has context tables, but not prediction controls, odds suggestions, lineup handling, fatigue controls, or recommendation outputs |
| League view | New app has standings only; old app also has team-level analytical detail |

### Not Covered Yet

| Feature | Gap |
|---|---|
| Player intelligence | No player pipeline or semantic marts yet |
| Injuries | No ingestion/modeling in DE project yet |
| Upcoming fixtures | No dedicated semantic product for future matches yet |
| Bet Builder | Not implemented in new app |
| Ticket export | Not implemented in new app |
| Live API fallback | Not implemented in new app |
| Language toggle | Not implemented in new app |
| App-side operational refresh UX | Not implemented in new app |

## Product Scope for Replacement

The replacement should be built in two levels.

### Level 1: Minimum Viable Replacement

Required before switching the live URL:

- overview page
- standings page
- match center page
- H2H page
- upcoming fixtures page
- stable semantic datasets for those pages
- clear freshness/status indicator in Streamlit

### Level 2: Full Parity Replacement

Required to match the existing app experience:

- player intelligence
- injuries visibility
- match prediction workflow
- auto-suggested odds
- bet builder
- ticket generation
- PDF export
- optional multilingual UX

## Target Semantic Data Products

The live app should eventually depend on a clear set of semantic datasets.

### Match Domain

- `semantic.silver_matches`
- `semantic.gold_match_context`
- `semantic.gold_h2h_context`
- `semantic.gold_standings`
- `semantic.gold_upcoming_fixtures`
- `semantic.gold_match_prediction_features`

### Player Domain

- `semantic.silver_player_stats`
- `semantic.silver_player_contributions`
- `semantic.silver_injuries`
- `semantic.gold_player_team_intelligence`
- `semantic.gold_player_scorer_candidates`
- `semantic.gold_player_cards_candidates`

### Operations Domain

- `semantic.pipeline_run`
- `semantic.file_manifest`
- `semantic.dim_league`
- `semantic.dim_team`

## Data Products Needed by App Area

### Overview

Needs:

- latest run metadata
- row counts
- league coverage
- season coverage
- last match date
- freshness indicators

Primary sources:

- `semantic.pipeline_run`
- `semantic.silver_matches`

### Standings

Needs:

- current season team standings
- points
- wins / draws / losses
- goal difference
- table position

Primary source:

- `semantic.gold_standings`

### Match Center

Needs:

- home/away team context
- recent form
- recent goals
- standing positions
- H2H metrics
- bookmaker odds
- prediction features
- optional lineup adjustments

Primary sources:

- `semantic.gold_match_context`
- `semantic.gold_h2h_context`
- future `semantic.gold_match_prediction_features`

### Upcoming Fixtures

Needs:

- future fixtures by league/date
- normalized teams
- source attribution
- freshness timestamp

Primary source:

- future `semantic.gold_upcoming_fixtures`

### Player Intelligence

Needs:

- injured players
- likely scorers
- likely cards
- player contributions by team
- team-level player impact

Primary sources:

- future `semantic.gold_player_team_intelligence`
- future `semantic.gold_player_scorer_candidates`
- future `semantic.gold_player_cards_candidates`

### Bet Builder

Needs:

- upcoming fixtures
- per-market probability outputs
- match-level recommendation features
- player scorer candidates
- market estimation logic

Primary sources:

- `semantic.gold_upcoming_fixtures`
- `semantic.gold_match_prediction_features`
- player Gold marts

## Migration Strategy

The migration should happen by domain, not by copying UI widgets one by one.

### Phase 1: Stabilize Current Match Analytics Foundation

Goal:

Make the current DE app a reliable replacement for the analytical read-only parts.

Deliverables:

- keep `silver_matches`, `gold_match_context`, `gold_h2h_context`, `gold_standings` stable
- improve Streamlit layout and navigation
- add freshness indicators and last successful pipeline information
- validate production deployment path for Streamlit + Dremio access

Exit criteria:

- overview, standings, H2H, and match context are production-stable

### Phase 2: Build Upcoming Fixtures Data Product

Goal:

Support future matches in a platform-native way.

Deliverables:

- ingestion for upcoming fixtures
- Bronze landing for fixtures source
- Silver standardization for upcoming fixtures
- Gold mart for app consumption:
  - `gold_upcoming_fixtures`
- Dremio semantic exposure
- Streamlit fixtures page

Possible sources:

- ESPN scoreboard
- API-Football
- fallback local or historical scheduling data if needed

Exit criteria:

- app can show future matches by date and league without relying on local CSV logic

### Phase 3: Rebuild Match Center as a Platform Consumer

Goal:

Move the old Match Center logic into platform-backed datasets plus minimal app logic.

Deliverables:

- semantic datasets that expose:
  - form metrics
  - standings context
  - H2H context
  - odds
  - prediction features
- a redesigned Match Center page in Streamlit
- same-league and cross-league selection
- model-driven prediction output
- rationale display based on feature gaps

Important rule:

Heavy feature engineering should live in dbt or a separate prediction service, not inside Streamlit callbacks.

Exit criteria:

- Match Center parity with the old app for match analysis and recommendation support

### Phase 4: Add Player Domain

Goal:

Support the player and injuries capabilities from the old app.

Deliverables:

- ingest player stats
- ingest injuries and player contributions
- create Silver models for player entities
- create Gold marts for:
  - player intelligence by team
  - likely scorers
  - likely cards
  - injury summaries
- add player intelligence sections to Streamlit

Exit criteria:

- the app can show injured players and player recommendation tables without local CSVs

### Phase 5: Rebuild Bet Builder

Goal:

Recreate the main live-app product flow using the DE platform as the backend.

Deliverables:

- page for date-range + league selection
- fetch or load upcoming fixtures through semantic datasets
- market selection workflow
- probability estimation workflow
- ticket generation
- conservative / moderate / high-risk views
- export support

Important rule:

The ticket-generation UI may remain in Streamlit, but probability inputs should come from curated datasets or a prediction service, not ad hoc local feature code.

Exit criteria:

- user can generate tickets from platform-backed inputs

### Phase 6: Production Hardening and Cutover

Goal:

Replace the live app safely.

Deliverables:

- environment configuration for hosted deployment
- Dremio connectivity validation from deployment target
- secret handling for any external APIs
- graceful error handling
- loading states and retry behavior
- deployment checklist
- rollback plan

Exit criteria:

- new app is ready to replace the current Streamlit deployment

## Recommended Build Order

1. Improve current Streamlit app structure and UX.
2. Build `gold_upcoming_fixtures`.
3. Rebuild Match Center.
4. Build player pipelines and player Gold marts.
5. Rebuild Bet Builder.
6. Add export and production hardening.
7. Replace the live app.

## Must-Have Before Live Replacement

- semantic support for current standings, H2H, match context, and upcoming fixtures
- stable Streamlit pages for those domains
- production-safe Dremio connectivity
- clear freshness metadata shown in the app
- no dependency on local CSVs for primary app flows

## Should-Have Before Live Replacement

- player intelligence
- model-based match recommendation output
- better app styling
- deployment monitoring

## Can Wait Until After Replacement

- multilingual UX
- PDF export
- advanced ticket rotation logic
- richer scenario simulation inputs

## Technical Workstreams

### Workstream 1: Data Engineering

- add new ingestion jobs
- add metadata tracking
- add Silver and Gold dbt models
- add tests and docs
- keep semantic contracts stable

### Workstream 2: Semantic Layer

- expose stable dataset names in Dremio
- avoid app coupling to raw storage paths
- ensure refreshed datasets are visible after scheduled runs

### Workstream 3: Application

- rebuild pages on semantic datasets
- keep app logic thin
- isolate UI-only logic from feature-engineering logic

### Workstream 4: Prediction

- decide whether prediction remains inside Streamlit, moves to a service, or is precomputed
- keep model feature generation outside the UI where possible

## Open Decisions

These decisions should be made explicitly before full parity work starts:

1. Will prediction stay inside the Streamlit app or move to a separate service/module?
2. Will upcoming fixtures be sourced from ESPN, API-Football, or both?
3. Will player data use the same sources as the old app or a revised source set?
4. Will the first live replacement ship without Bet Builder, or is Bet Builder mandatory for cutover?

## Recommendation

For the cleanest migration, do not try to replace the live app in one step.

Recommended release sequence:

1. Deploy the new DE app as an internal or staging version.
2. Reach parity for:
   - standings
   - H2H
   - match context
   - upcoming fixtures
3. Add player domain.
4. Add Bet Builder.
5. Replace the live app once the critical user journeys are validated.

## Success Definition

This migration is successful when:

- the app no longer depends on local processed CSV files for core workflows
- the app reads stable semantic datasets from the DE platform
- scheduled pipelines refresh the data used by the app
- core user journeys from the old app are available in the new app
- the hosted Streamlit deployment can be switched with acceptable risk

## Immediate Next Step

The next implementation step should be:

**build `gold_upcoming_fixtures` and a dedicated Upcoming Fixtures page in the new Streamlit app**

Reason:

- it closes one of the largest parity gaps
- it is required for both Match Center and Bet Builder
- it fits the DE architecture better than jumping directly into UI-heavy ticket generation
