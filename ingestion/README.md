# Ingestion

This folder contains the Python extraction and raw loading logic for the MVP source.

Current behavior:

- download raw CSVs from `football-data.co.uk`
- land the files locally first under `data/raw/...`
- upload the same files to MinIO Bronze
- register run and file metadata in PostgreSQL
- support one current season plus configurable prior seasons in the same run

Responsibility:

- read from external sources
- stage raw files locally
- write raw data to Bronze
- register metadata in PostgreSQL
