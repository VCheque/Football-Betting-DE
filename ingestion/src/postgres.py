from __future__ import annotations

import os

import psycopg

from .models import FileManifest, PipelineRun


def connect_postgres():
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "football_platform"),
        user=os.getenv("POSTGRES_USER", "football"),
        password=os.getenv("POSTGRES_PASSWORD", "football"),
        autocommit=True,
    )


def insert_pipeline_run(conn, pipeline_run: PipelineRun) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_run (
                run_id,
                source_name,
                entity_name,
                status,
                started_at,
                completed_at,
                row_count,
                checksum,
                error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                pipeline_run.run_id,
                pipeline_run.source_name,
                pipeline_run.entity_name,
                pipeline_run.status,
                pipeline_run.started_at,
                pipeline_run.completed_at,
                pipeline_run.row_count,
                pipeline_run.checksum,
                pipeline_run.error_message,
            ),
        )


def update_pipeline_run(conn, pipeline_run: PipelineRun) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_run
            SET status = %s,
                completed_at = %s,
                row_count = %s,
                checksum = %s,
                error_message = %s
            WHERE run_id = %s
            """,
            (
                pipeline_run.status,
                pipeline_run.completed_at,
                pipeline_run.row_count,
                pipeline_run.checksum,
                pipeline_run.error_message,
                pipeline_run.run_id,
            ),
        )


def insert_file_manifest(conn, file_manifest: FileManifest) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO file_manifest (
                run_id,
                bucket_name,
                object_key,
                file_name,
                source_url,
                checksum,
                byte_size,
                row_count
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                file_manifest.run_id,
                file_manifest.bucket_name,
                file_manifest.object_key,
                file_manifest.file_name,
                file_manifest.source_url,
                file_manifest.checksum,
                file_manifest.byte_size,
                file_manifest.row_count,
            ),
        )


def upsert_dim_league(conn, league_code: str, league_name: str, country_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dim_league (
                source_league_key,
                league_name,
                country_name
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (source_league_key)
            DO UPDATE SET
                league_name = EXCLUDED.league_name,
                country_name = EXCLUDED.country_name,
                updated_at = NOW()
            """,
            (league_code, league_name, country_name),
        )
