# Dremio

This folder contains Dremio semantic-layer assets for the MVP.

Current contents:

- `sql/semantic_raw_matches_odds.sql`
- `sql/semantic_silver_matches.sql`

What this SQL does:

- reads the 6 Bronze CSV files directly from MinIO with Dremio `TABLE(...csv(type => 'text'))`
- keeps only the stable shared raw columns needed for the MVP
- adds semantic metadata columns such as `Season`, `RunId`, and `IngestDate`
- creates a stable raw dataset contract for dbt as `semantic.raw_matches_odds`
- exposes the latest published Silver Parquet artifact as `semantic.silver_matches`

Responsibility:

- define how MinIO and PostgreSQL are exposed through one query layer
