"""Upload a manual injuries CSV to MinIO Bronze and register it in PostgreSQL.

Injury data has no reliable free API, so it is maintained manually.  This
script acts as the upload tool: drop your updated injuries.csv file locally
and run this script to push it into the Bronze layer and refresh metadata.

Expected CSV columns (any extra columns are passed through):
    player_name, team, league_code, injury_type, return_date (YYYY-MM-DD or empty)

Usage:
    python -m src.upload_injuries --file path/to/injuries.csv

After running, execute sync_semantic_layer.py with --entity-name injuries and
then `dbt run --select stg_injuries gold_injuries` to refresh the semantic layer.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from .models import FileManifest, PipelineRun
from .config import BRONZE_PREFIX, DEFAULT_BUCKET

INJURIES_SOURCE_NAME = "manual"
INJURIES_ENTITY_NAME = "injuries"

REQUIRED_COLUMNS = {"player_name", "team", "league_code", "injury_type"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload a manual injuries CSV to MinIO Bronze and register metadata."
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Path to the injuries CSV file to upload.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print the payload without writing to MinIO or PostgreSQL.",
    )
    return parser.parse_args()


def validate_csv(content_bytes: bytes) -> int:
    """Return the data row count, raise if required columns are missing."""
    reader = csv.DictReader(io.StringIO(content_bytes.decode("utf-8", errors="replace")))
    headers = set(reader.fieldnames or [])
    missing = REQUIRED_COLUMNS - headers
    if missing:
        raise ValueError(f"Injuries CSV is missing required columns: {sorted(missing)}")
    return sum(1 for _ in reader)


def bronze_object_key(ingest_date: str, run_id: str, file_name: str) -> str:
    return (
        f"{BRONZE_PREFIX}/source={INJURIES_SOURCE_NAME}/entity={INJURIES_ENTITY_NAME}/"
        f"ingest_date={ingest_date}/run_id={run_id}/{file_name}"
    )


def main() -> int:
    args = parse_args()
    file_path = Path(args.file)
    if not file_path.exists():
        print(f"ERROR: File not found: {file_path}")
        return 1

    content_bytes = file_path.read_bytes()
    row_count = validate_csv(content_bytes)
    checksum = hashlib.sha256(content_bytes).hexdigest()
    run_id = str(uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    ingest_date = datetime.now(timezone.utc).date().isoformat()
    bucket_name = os.getenv("MINIO_BUCKET", DEFAULT_BUCKET)
    file_name = "injuries.csv"
    object_key = bronze_object_key(ingest_date, run_id, file_name)

    pipeline_run = PipelineRun(
        run_id=run_id,
        source_name=INJURIES_SOURCE_NAME,
        entity_name=INJURIES_ENTITY_NAME,
        status="prepared" if args.dry_run else "completed",
        row_count=row_count,
        checksum=checksum,
        started_at=started_at,
        completed_at=datetime.now(timezone.utc).isoformat(),
        error_message=None,
    )

    file_manifest = FileManifest(
        run_id=run_id,
        bucket_name=bucket_name,
        object_key=object_key,
        file_name=file_name,
        source_url=str(file_path.resolve()),
        checksum=checksum,
        byte_size=len(content_bytes),
        row_count=row_count,
    )

    if not args.dry_run:
        from .postgres import (
            connect_postgres,
            insert_file_manifest,
            insert_pipeline_run,
        )
        from .storage import build_s3_client, ensure_bucket, upload_raw_file

        conn = connect_postgres()
        s3_client = build_s3_client()
        try:
            insert_pipeline_run(conn, pipeline_run)
            ensure_bucket(s3_client, bucket_name)
            upload_raw_file(
                client=s3_client,
                bucket_name=bucket_name,
                object_key=object_key,
                content_bytes=content_bytes,
                checksum=checksum,
            )
            insert_file_manifest(conn, file_manifest)
        finally:
            conn.close()

    payload = {
        "pipeline_run": pipeline_run.to_dict(),
        "file_manifest": file_manifest.to_dict(),
        "row_count": row_count,
        "dry_run": args.dry_run,
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
