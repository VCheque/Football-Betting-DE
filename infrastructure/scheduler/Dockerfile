FROM python:3.11-slim

ARG SUPERCRONIC_VERSION=v0.2.33
ARG TARGETARCH

WORKDIR /workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN case "${TARGETARCH}" in \
        amd64) SUPERCRONIC_ARCH="amd64" ;; \
        arm64) SUPERCRONIC_ARCH="arm64" ;; \
        *) echo "Unsupported TARGETARCH: ${TARGETARCH}" && exit 1 ;; \
    esac \
    && curl -fsSLo /usr/local/bin/supercronic \
        "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-${SUPERCRONIC_ARCH}" \
    && chmod +x /usr/local/bin/supercronic

COPY ingestion/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY dbt/requirements.txt /tmp/dbt-requirements.txt
RUN pip install --no-cache-dir -r /tmp/dbt-requirements.txt

COPY infrastructure/scheduler/cron/ingestion-crontab /etc/cron.d/ingestion-crontab
COPY infrastructure/scheduler/scripts/run_scheduled_ingestion.sh /usr/local/bin/run-scheduled-ingestion.sh
COPY infrastructure/scheduler/scripts/run_scheduled_player_stats.sh /usr/local/bin/run-scheduled-player-stats.sh
COPY infrastructure/scheduler/scripts/run_scheduled_upcoming_fixtures.sh /usr/local/bin/run-scheduled-upcoming-fixtures.sh

RUN chmod 0644 /etc/cron.d/ingestion-crontab \
    && chmod +x /usr/local/bin/run-scheduled-ingestion.sh \
    && chmod +x /usr/local/bin/run-scheduled-player-stats.sh \
    && chmod +x /usr/local/bin/run-scheduled-upcoming-fixtures.sh

ENTRYPOINT ["/usr/local/bin/supercronic", "/etc/cron.d/ingestion-crontab"]
