# Dremio

This folder contains Dremio semantic-layer assets for the MVP.

Current contents:

- `sql/semantic_raw_matches_odds.sql`

What this SQL does:

- reads the 6 Bronze CSV files directly from MinIO with Dremio `TABLE(...csv(type => 'text'))`
- keeps only the stable shared raw columns needed for the MVP
- adds semantic metadata columns such as `Season`, `RunId`, and `IngestDate`
- creates a stable raw dataset contract for dbt as `semantic.raw_matches_odds`

dbt then creates:

- `semantic.silver_matches`
- `semantic.gold_match_context`
- `semantic.gold_h2h_context`

Responsibility:

- define how MinIO and PostgreSQL are exposed through one query layer
