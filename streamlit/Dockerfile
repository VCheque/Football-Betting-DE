FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies first (layer-cached unless requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Dremio client and data loader
COPY dremio_client.py .
COPY dremio_data_loader.py .

# Copy the main Streamlit app and the sports_betting package it depends on.
# The sports_betting package is vendored from the Football Bets project so the
# ML models and bet-combination logic work without a separate install step.
COPY app.py .
COPY sports_betting/ ./sports_betting/

EXPOSE 8501

# Health check — Streamlit responds on /_stcore/health
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')"

ENV STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]
