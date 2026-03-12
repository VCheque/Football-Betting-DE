"""Entry point for Understat player-stats ingestion.

Mirrors the structure of main.py (football-data.co.uk ingestion) but targets
the Understat source.  Only the five Understat-supported leagues are processed;
Primeira Liga (P1) is silently skipped.

Usage:
    python -m src.run_player_stats \\
        --season-start 2024 \\
        --history-seasons 1

CLI flags match main.py for scheduler compatibility.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

from .football_data_co_uk import compute_checksum, count_csv_rows
from .understat_player_stats import (
    UNDERSTAT_LEAGUES,
    build_file_manifest,
    build_pipeline_run,
    fetch_player_stats,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Understat player stats into Bronze and PostgreSQL metadata."
    )
    parser.add_argument(
        "--league-code",
        help=(
            "Optional single football-data.co.uk league code, e.g. E0 or SP1. "
            "If omitted, all five Understat-supported leagues are ingested. "
            "Primeira Liga (P1) is always skipped — not available on Understat."
        ),
    )
    parser.add_argument(
        "--season-start",
        type=int,
        required=True,
        help="Season start year, e.g. 2024 for the 2024/25 season.",
    )
    parser.add_argument(
        "--history-seasons",
        type=int,
        default=0,
        help="Number of prior seasons to include. E.g. --season-start 2025 --history-seasons 1 loads 2025/26 and 2024/25.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Prepare payloads without writing to MinIO or PostgreSQL.",
    )
    return parser.parse_args()


def selected_league_codes(league_code: str | None) -> tuple[str, ...]:
    if league_code:
        if league_code not in UNDERSTAT_LEAGUES:
            raise ValueError(
                f"League code '{league_code}' is not available on Understat. "
                f"Supported codes: {sorted(UNDERSTAT_LEAGUES)}"
            )
        return (league_code,)
    return tuple(UNDERSTAT_LEAGUES)


def selected_season_starts(season_start: int, history_seasons: int) -> tuple[int, ...]:
    if history_seasons < 0:
        raise ValueError("history_seasons must be zero or greater")
    return tuple(season_start - offset for offset in range(history_seasons + 1))


def aggregate_checksum(checksums: list[str]) -> str:
    joined = "|".join(checksums).encode("utf-8")
    return hashlib.sha256(joined).hexdigest()


def main() -> int:
    args = parse_args()
    league_codes = selected_league_codes(args.league_code)
    season_starts = selected_season_starts(args.season_start, args.history_seasons)
    run_id = str(uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    ingest_date = datetime.now(timezone.utc).date().isoformat()
    bucket_name = os.getenv("MINIO_BUCKET", "football")
    prepared_files = []

    from .storage import write_local_raw_file

    for season_start in season_starts:
        for league_code in league_codes:
            print(f"Fetching {league_code} {season_start}/{season_start + 1} …")
            source_file = fetch_player_stats(
                league_code=league_code,
                season_start=season_start,
            )
            local_path = write_local_raw_file(
                run_id=run_id,
                ingest_date=ingest_date,
                league_code=league_code,
                season_start=season_start,
                file_name=source_file.file_name,
                content_bytes=source_file.content_bytes,
            )
            source_file = replace(source_file, local_path=str(local_path))
            checksum = compute_checksum(source_file.content_bytes)
            row_count = count_csv_rows(source_file.content_bytes)
            file_manifest = build_file_manifest(
                run_id=run_id,
                source_file=source_file,
                checksum=checksum,
                row_count=row_count,
                league_code=league_code,
                season_start=season_start,
                ingest_date=ingest_date,
                bucket_name=bucket_name,
            )
            prepared_files.append(
                {
                    "league_code": league_code,
                    "season_start": season_start,
                    "source_file": source_file,
                    "checksum": checksum,
                    "row_count": row_count,
                    "file_manifest": file_manifest,
                }
            )

    checksums = [item["checksum"] for item in prepared_files]
    total_row_count = sum(item["row_count"] for item in prepared_files)
    completed_at = datetime.now(timezone.utc).isoformat()
    final_pipeline_run = build_pipeline_run(
        run_id=run_id,
        checksum=aggregate_checksum(checksums),
        row_count=total_row_count,
        started_at=started_at,
        completed_at=completed_at,
        status="prepared" if args.dry_run else "completed",
    )

    if not args.dry_run:
        from .postgres import (
            connect_postgres,
            insert_file_manifest,
            insert_pipeline_run,
            update_pipeline_run,
        )
        from .storage import build_s3_client, ensure_bucket, upload_raw_file

        running_pipeline_run = build_pipeline_run(
            run_id=run_id,
            checksum=None,
            row_count=None,
            started_at=started_at,
            completed_at=None,
            status="running",
        )
        conn = connect_postgres()
        s3_client = build_s3_client()

        try:
            insert_pipeline_run(conn, running_pipeline_run)
            ensure_bucket(s3_client, bucket_name)
            for item in prepared_files:
                source_file = item["source_file"]
                file_manifest = item["file_manifest"]
                upload_raw_file(
                    client=s3_client,
                    bucket_name=file_manifest.bucket_name,
                    object_key=file_manifest.object_key,
                    content_bytes=source_file.content_bytes,
                    checksum=item["checksum"],
                )
                insert_file_manifest(conn, file_manifest)
            update_pipeline_run(conn, final_pipeline_run)
        except Exception as exc:
            failed_pipeline_run = build_pipeline_run(
                run_id=run_id,
                checksum=None,
                row_count=None,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc).isoformat(),
                status="failed",
                error_message=str(exc),
            )
            update_pipeline_run(conn, failed_pipeline_run)
            raise
        finally:
            conn.close()

    payload = {
        "pipeline_run": final_pipeline_run.to_dict(),
        "files": [
            {
                "league_code": item["league_code"],
                "season_start": item["season_start"],
                "local_path": item["source_file"].local_path,
            }
            for item in prepared_files
        ],
        "file_manifests": [item["file_manifest"].to_dict() for item in prepared_files],
        "dry_run": args.dry_run,
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
