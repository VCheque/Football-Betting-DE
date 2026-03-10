INSERT INTO source_config (
    source_name,
    entity_name,
    is_enabled,
    cadence,
    config_json
)
VALUES (
    'football_data_co_uk',
    'matches_odds',
    TRUE,
    'manual_mvp',
    '{"base_url": "https://www.football-data.co.uk/mmz4281"}'::jsonb
)
ON CONFLICT (source_name, entity_name) DO NOTHING;
