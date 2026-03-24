# Football Betting Data Platform

End-to-end data engineering platform that ingests raw football match data, transforms it through a Bronze → Silver → Gold medallion pipeline, and serves an XGBoost-powered match prediction app — fully containerised and refreshed automatically.

---

## Architecture

<div align="center">

<br/>

<table>
  <tr>
    <td align="center" colspan="5">
      <b>Data Sources</b><br/>
      <sup>football-data.co.uk &nbsp;·&nbsp; Understat &nbsp;·&nbsp; ESPN &nbsp;·&nbsp; Manual CSV</sup>
    </td>
  </tr>
  <tr><td colspan="5" align="center"><br/>⬇<br/><br/></td></tr>
  <tr>
    <td align="center" colspan="2">
      <img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/python/python-original.svg" width="52" alt="Python"/><br/>
      <b>Ingestion</b><br/>
      <sup>Python jobs · cron scheduler</sup>
    </td>
    <td width="40"></td>
    <td align="center" colspan="2">
      <img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/docker/docker-original.svg" width="52" alt="Docker"/><br/>
      <b>Container Stack</b><br/>
      <sup>Docker Compose</sup>
    </td>
  </tr>
  <tr><td colspan="5" align="center"><br/>⬇<br/><br/></td></tr>
  <tr>
    <td align="center">
      <img src="https://cdn.simpleicons.org/minio/C72E49" width="52" alt="MinIO"/><br/>
      <b>Bronze</b><br/>
      <sup>MinIO · raw snapshots</sup>
    </td>
    <td width="30" align="center">⟷</td>
    <td align="center">
      <img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/postgresql/postgresql-original.svg" width="52" alt="PostgreSQL"/><br/>
      <b>Control Plane</b><br/>
      <sup>PostgreSQL · metadata · dims</sup>
    </td>
    <td width="30" align="center"></td>
    <td></td>
  </tr>
  <tr><td colspan="5" align="center"><br/>⬇<br/><br/></td></tr>
  <tr>
    <td align="center" colspan="5">
      <img src="https://avatars.githubusercontent.com/u/12727786?v=4" width="52" alt="Dremio"/><br/>
      <b>Semantic Layer</b><br/>
      <sup>Dremio · federated SQL over MinIO + PostgreSQL</sup>
    </td>
  </tr>
  <tr><td colspan="5" align="center"><br/>⬇<br/><br/></td></tr>
  <tr>
    <td align="center" colspan="5">
      <img src="https://cdn.simpleicons.org/dbt/FF694B" width="52" alt="dbt"/><br/>
      <b>Transformations</b><br/>
      <sup>dbt · staging → silver → gold · seeds · tests</sup>
    </td>
  </tr>
  <tr><td colspan="5" align="center"><br/>⬇<br/><br/></td></tr>
  <tr>
    <td align="center" colspan="2">
      <img src="https://cdn.simpleicons.org/scikitlearn/F7931E" width="52" alt="XGBoost"/><br/>
      <b>ML Model</b><br/>
      <sup>XGBoost · 19 features · Platt calibration</sup>
    </td>
    <td width="40"></td>
    <td align="center" colspan="2">
      <img src="https://cdn.simpleicons.org/streamlit/FF4B4B" width="52" alt="Streamlit"/><br/>
      <b>App</b><br/>
      <sup>Streamlit · Dremio REST client</sup>
    </td>
  </tr>
</table>

<br/>

</div>

---

## Stack

| | Tool | Role |
|:---:|---|---|
| <img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/python/python-original.svg" width="24"/> | **Python** | Ingestion jobs for match data (football-data.co.uk), player stats (Understat), and upcoming fixtures (ESPN). Each job assigns a run ID, computes a SHA-256 checksum, and registers file manifests before writing to storage. |
| <img src="https://cdn.simpleicons.org/minio/C72E49" width="24"/> | **MinIO** | S3-compatible object store for immutable Bronze snapshots. Partitioned by `source / entity / ingest_date / run_id / league / season`. Append-only — no file is ever overwritten. |
| <img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/postgresql/postgresql-original.svg" width="24"/> | **PostgreSQL** | Control plane: ingestion run records (`pipeline_run`), file manifests, checksums, and reference dimensions (`dim_league`, `dim_team`, `dim_derby_pairs`). |
| <img src="https://avatars.githubusercontent.com/u/12727786?v=4" width="24"/> | **Dremio** | Federates MinIO and PostgreSQL into a single SQL surface. The app and dbt always query through Dremio — never directly against storage. |
| <img src="https://cdn.simpleicons.org/dbt/FF694B" width="24"/> | **dbt** | All transformation logic. Staging → Silver → Gold chain with `not_null`, `unique`, and `accepted_values` tests. Seeds manage static reference data. |
| <img src="https://cdn.simpleicons.org/scikitlearn/F7931E" width="24"/> | **XGBoost** | Multi-class match outcome classifier (`H/D/A`). 19-feature vector including suspension and key-player impact gaps. Platt sigmoid calibration on a time-ordered holdout. Retrained at app load from Dremio Gold data. |
| <img src="https://cdn.simpleicons.org/streamlit/FF4B4B" width="24"/> | **Streamlit** | Three-tab app: Match Centre (predictions + EV framework), Team Stats, League & Players. Reads exclusively from `semantic.gold_*` and `semantic.silver_*` views via Dremio's REST Jobs API. |
| <img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/docker/docker-original.svg" width="24"/> | **Docker Compose** | Full local stack in a single `docker compose up`. Ingestion and dbt use the `jobs` profile; the app uses the `app` profile. |

---

## Ingestion Schedule

| Job | Source | Frequency | Entity |
|---|---|---|---|
| Match odds | football-data.co.uk | 4× daily (00:15, 06:15, 12:15, 18:15 UTC) | `matches_odds` |
| Player stats | Understat | Weekly (Sunday 02:00 UTC) | `player_stats` |
| Upcoming fixtures | ESPN Scoreboard API | Every 2 hours | `upcoming_fixtures` |
| Injuries | Manual CSV upload | On demand | `injuries` |

---

## dbt Models

### Silver Layer

| Model | What it produces |
|---|---|
| `silver_matches` | Cleaned and standardised match records with full-time results, goals, shots, cards, corners, and odds |
| `silver_upcoming_fixtures` | Deduplicated fixture schedule from ESPN — upcoming and recently completed matches for all 6 leagues |

### Gold Layer

| Model | What it produces |
|---|---|
| `gold_match_context` | Rolling 5-game form per team: points/game, goals, corners, cards, shots-on-target diff, momentum slope |
| `gold_h2h_context` | Exponentially decayed head-to-head win rates and goal differential per matchup |
| `gold_standings` | Live season table: position, PPG, W-D-L, GF, GA, GD |
| `gold_rest_fatigue` | Days since last match + matches played in the previous 21 days per team |
| `gold_team_season_stats` | Stats split by scope: all / home / away / vs top-half / vs bottom-half |
| `gold_player_stats` | Per-player rates per 90 min: xG, xA, goals, assists, key passes, cards |
| `gold_injuries` | Active injury records from manually uploaded CSVs |

---

## App Features

### Match Centre
- League and team selectors across all 6 leagues (Premier League, La Liga, Serie A, Bundesliga, Ligue 1, Primeira Liga)
- XGBoost outcome probabilities (`H / D / A`) with EV-based pick recommendations at three risk tiers (Conservative, Moderate, High Risk)
- Auto-suggested odds from the model (5% margin)
- Suspension alerts: yellow-card accumulation tracking + red-card bans flagged per team before prediction
- Key player impact score per team derived from season per-90 xG/goals/assists
- Player intel panel: active injuries, likely scorers, likely card candidates — sourced from `gold_player_stats` when per-match contrib data is unavailable

### Team Stats
- Head-to-head analytics with configurable year window and home/away/all scope
- Rolling form charts and season aggregate comparisons

### League & Players
- Full standings table: position, points, GD, form, home/away PPG, injury count, suspension count, key player impact
- Per-player XGBoost scoring/carding probabilities
- Team injury and player performance summary

### Bet Builder
- Multi-league fixture selector with configurable date range
- Fixture source priority: Platform DB (Dremio `silver_upcoming_fixtures`) → ESPN live → API-Football → local dataset
- Accumulator ticket builder with EV filter and multi-market support

### Sidebar
- Pipeline freshness panel: last successful run timestamp per entity, row count, and staleness indicator (green < 6h / yellow < 24h / red ≥ 24h)
- Production mode warnings when Dremio is unreachable or silver tables are empty

---

## Clone & Run

```bash
git clone https://github.com/VCheque/Football-Betting-DE.git
cd Football-Betting-DE
cp infrastructure/.env.example infrastructure/.env
```

Edit `infrastructure/.env` with your credentials, then start the core services:

```bash
cd infrastructure
docker compose up -d postgres minio dremio
```

Seed the database and build the Gold models:

```bash
docker compose --profile jobs up -d dbt-runner
docker exec football-dbt dbt seed
docker exec football-dbt dbt run
```

Trigger the first ingestion run to populate Bronze and Silver:

```bash
docker compose --profile jobs run --rm ingestion-runner python -m src.main
docker compose --profile jobs run --rm ingestion-runner python -m src.run_player_stats
docker compose --profile jobs run --rm ingestion-runner python -m src.run_upcoming_fixtures
```

Start the app:

```bash
docker compose --profile app up -d streamlit
# → http://localhost:8501
```
