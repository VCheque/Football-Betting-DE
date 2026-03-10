# Infrastructure

This folder contains the local platform setup for the project.

Current contents:

- `docker-compose.yml`
- environment templates
- scheduler assets
- bootstrap files for local services

Notable implementation:

- a dedicated `scheduler` service runs the ingestion job four times per day
- the schedule is defined with cron syntax under `scheduler/cron/`
- the scheduler uses the same ingestion codebase and Docker network as the rest of the platform

Responsibility:

- define how the platform runs locally
- keep infrastructure separate from ingestion and transformation code
