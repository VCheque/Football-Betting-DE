CREATE TABLE IF NOT EXISTS source_config (
    source_config_id SERIAL PRIMARY KEY,
    source_name TEXT NOT NULL,
    entity_name TEXT NOT NULL,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    cadence TEXT,
    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_name, entity_name)
);

CREATE TABLE IF NOT EXISTS dim_league (
    league_id SERIAL PRIMARY KEY,
    source_league_key TEXT UNIQUE,
    league_name TEXT NOT NULL,
    country_name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_team (
    team_id SERIAL PRIMARY KEY,
    source_team_key TEXT UNIQUE,
    team_name TEXT NOT NULL,
    short_name TEXT,
    country_name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS team_alias_map (
    alias_id SERIAL PRIMARY KEY,
    team_id INTEGER NOT NULL REFERENCES dim_team(team_id),
    source_name TEXT NOT NULL,
    alias_name TEXT NOT NULL,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_name, alias_name)
);

CREATE TABLE IF NOT EXISTS pipeline_run (
    run_id UUID PRIMARY KEY,
    source_name TEXT NOT NULL,
    entity_name TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    row_count BIGINT,
    checksum TEXT,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS file_manifest (
    file_manifest_id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES pipeline_run(run_id),
    bucket_name TEXT NOT NULL,
    object_key TEXT NOT NULL,
    file_name TEXT NOT NULL,
    source_url TEXT,
    checksum TEXT,
    byte_size BIGINT,
    row_count BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dq_results (
    dq_result_id BIGSERIAL PRIMARY KEY,
    run_id UUID REFERENCES pipeline_run(run_id),
    check_name TEXT NOT NULL,
    check_level TEXT NOT NULL,
    status TEXT NOT NULL,
    affected_rows BIGINT,
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_source_entity
    ON pipeline_run (source_name, entity_name, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_file_manifest_run
    ON file_manifest (run_id);

CREATE INDEX IF NOT EXISTS idx_dq_results_run
    ON dq_results (run_id);
