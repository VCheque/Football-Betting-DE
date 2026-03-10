# Streamlit

This folder will contain the application layer.

Current contents:

- `app.py`
- `requirements.txt`
- `Dockerfile`

Current views:

- overview metrics from `semantic.silver_matches` and `semantic.pipeline_run`
- standings explorer from `semantic.gold_standings`
- match context explorer from `semantic.gold_match_context`
- H2H explorer from `semantic.gold_h2h_context`

Responsibility:

- consume curated datasets
- present analytics outputs
- avoid heavy transformation logic
