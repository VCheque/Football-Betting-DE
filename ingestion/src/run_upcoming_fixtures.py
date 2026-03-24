"""Entry point for ESPN upcoming-fixtures ingestion.

Fetches the next N days of fixtures (and recent completed results) from the
ESPN scoreboard API for all six configured leagues (including Primeira Liga,
which IS available on ESPN).  Stores one CSV per league in MinIO Bronze and
registers the run in the PostgreSQL metadata tables.

Mirrors the structure of run_player_stats.py.

Usage:
    python -m src.run_upcoming_fixtures
    python -m src.run_upcoming_fixtures --lookahead-days 14 --dry-run
    python -m src.run_upcoming_fixtures --league-code E0
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen
from uuid import uuid4

from .config import BRONZE_PREFIX, DEFAULT_BUCKET
from .football_data_co_uk import compute_checksum, count_csv_rows
from .models import FileManifest, PipelineRun, SourceFile


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

ESPN_SOURCE_NAME = "espn"
ESPN_ENTITY_NAME = "upcoming_fixtures"

# football-data.co.uk league code → ESPN soccer slug
ESPN_SLUGS: dict[str, str] = {
    "E0":  "eng.1",
    "SP1": "esp.1",
    "I1":  "ita.1",
    "D1":  "ger.1",
    "F1":  "fra.1",
    "P1":  "por.1",
}

# CSV columns written to MinIO Bronze
CSV_COLUMNS: tuple[str, ...] = (
    "match_date",
    "league_code",
    "home_team",
    "away_team",
    "result_ft",
    "espn_status",
)

# Default fetch window: 3 days before today through lookahead days ahead
DEFAULT_LOOKBACK_DAYS = 3
DEFAULT_LOOKAHEAD_DAYS = 14


# ─────────────────────────────────────────────────────────────────────────────
# ESPN fetch helpers
# ─────────────────────────────────────────────────────────────────────────────

def _espn_scoreboard_url(slug: str, date_str: str) -> str:
    """Build ESPN scoreboard URL for one league and one calendar date (YYYYMMDD)."""
    return (
        f"https://site.api.espn.com/apis/site/v2/sports/soccer/{slug}/scoreboard"
        f"?dates={date_str}"
    )


def _fetch_espn_day(slug: str, date_str: str) -> list[dict]:
    """Fetch all competitions for a league on a single date. Returns raw event dicts."""
    url = _espn_scoreboard_url(slug, date_str)
    try:
        with urlopen(url, timeout=15) as resp:  # noqa: S310
            data = json.loads(resp.read())
    except Exception as exc:  # noqa: BLE001
        print(f"  Warning: ESPN fetch failed for {slug} on {date_str}: {exc}")
        return []
    return data.get("events", [])


def _derive_result(home_score: str | int, away_score: str | int) -> str | None:
    """Return 'H', 'D', or 'A', or None on parse error."""
    try:
        hs = int(home_score)
        as_ = int(away_score)
        return "H" if hs > as_ else ("A" if as_ > hs else "D")
    except (TypeError, ValueError):
        return None


def _parse_event(event: dict, league_code: str) -> dict | None:
    """Parse one ESPN event dict into a CSV row dict. Returns None on parse failure."""
    try:
        competitions = event.get("competitions", [])
        if not competitions:
            return None
        comp = competitions[0]

        # Match date (YYYY-MM-DD)
        raw_date = event.get("date", "")[:10]  # "2025-03-16T..."
        if not raw_date:
            return None

        # Competitors — ESPN orders: index 0 = home, index 1 = away
        competitors = comp.get("competitors", [])
        home_c = next((c for c in competitors if c.get("homeAway") == "home"), None)
        away_c = next((c for c in competitors if c.get("homeAway") == "away"), None)
        if not home_c or not away_c:
            return None

        home_team = home_c.get("team", {}).get("displayName", "")
        away_team = away_c.get("team", {}).get("displayName", "")

        espn_status = comp.get("status", {}).get("type", {}).get("name", "")
        if espn_status == "STATUS_FULL_TIME":
            result_ft = _derive_result(home_c.get("score", ""), away_c.get("score", ""))
            result_ft = result_ft or ""
        else:
            result_ft = ""

        return {
            "match_date": raw_date,
            "league_code": league_code,
            "home_team": home_team,
            "away_team": away_team,
            "result_ft": result_ft,
            "espn_status": espn_status,
        }
    except Exception as exc:  # noqa: BLE001
        print(f"  Warning: failed to parse ESPN event: {exc}")
        return None


def fetch_upcoming_fixtures(
    league_code: str,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    lookahead_days: int = DEFAULT_LOOKAHEAD_DAYS,
) -> SourceFile:
    """Fetch fixtures for one league and return as a CSV SourceFile."""
    slug = ESPN_SLUGS[league_code]
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=lookback_days)
    end = today + timedelta(days=lookahead_days)

    rows: list[dict] = []
    current = start
    while current <= end:
        date_str = current.strftime("%Y%m%d")
        events = _fetch_espn_day(slug, date_str)
        for event in events:
            row = _parse_event(event, league_code)
            if row:
                rows.append(row)
        current += timedelta(days=1)

    content_bytes = _to_csv_bytes(rows)
    file_name = f"{league_code}_upcoming_fixtures.csv"
    source_url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/{slug}/scoreboard"
    return SourceFile(
        source_url=source_url,
        file_name=file_name,
        content_bytes=content_bytes,
    )


def _to_csv_bytes(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({col: row.get(col, "") for col in CSV_COLUMNS})
    return buf.getvalue().encode("utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# MinIO / metadata helpers  (mirror understat_player_stats.py pattern)
# ─────────────────────────────────────────────────────────────────────────────

def _current_season_start() -> int:
    """Derive current football season start year (rollover in August)."""
    now = datetime.now(timezone.utc)
    return now.year if now.month >= 8 else now.year - 1


def bronze_object_key(
    league_code: str,
    season_start: int,
    ingest_date: str,
    run_id: str,
    file_name: str,
) -> str:
    return (
        f"{BRONZE_PREFIX}/source={ESPN_SOURCE_NAME}/entity={ESPN_ENTITY_NAME}/"
        f"ingest_date={ingest_date}/run_id={run_id}/league={league_code}/"
        f"season={season_start}/{file_name}"
    )


def build_pipeline_run(
    run_id: str,
    checksum: str | None,
    row_count: int | None,
    started_at: str,
    completed_at: str | None,
    status: str,
    error_message: str | None = None,
) -> PipelineRun:
    return PipelineRun(
        run_id=run_id,
        source_name=ESPN_SOURCE_NAME,
        entity_name=ESPN_ENTITY_NAME,
        status=status,
        row_count=row_count,
        checksum=checksum,
        started_at=started_at,
        completed_at=completed_at,
        error_message=error_message,
    )


def build_file_manifest(
    run_id: str,
    source_file: SourceFile,
    checksum: str,
    row_count: int,
    league_code: str,
    season_start: int,
    ingest_date: str,
    bucket_name: str,
) -> FileManifest:
    object_key = bronze_object_key(
        league_code=league_code,
        season_start=season_start,
        ingest_date=ingest_date,
        run_id=run_id,
        file_name=source_file.file_name,
    )
    return FileManifest(
        run_id=run_id,
        bucket_name=bucket_name,
        object_key=object_key,
        file_name=source_file.file_name,
        source_url=source_file.source_url,
        checksum=checksum,
        byte_size=len(source_file.content_bytes),
        row_count=row_count,
    )


def aggregate_checksum(checksums: list[str]) -> str:
    joined = "|".join(checksums).encode("utf-8")
    return hashlib.sha256(joined).hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest ESPN upcoming fixtures into Bronze and PostgreSQL metadata."
    )
    parser.add_argument(
        "--league-code",
        help=(
            "Optional single football-data.co.uk league code, e.g. E0 or P1. "
            "If omitted, all six ESPN-supported leagues are ingested."
        ),
    )
    parser.add_argument(
        "--lookahead-days",
        type=int,
        default=DEFAULT_LOOKAHEAD_DAYS,
        help=f"Days ahead to fetch (default: {DEFAULT_LOOKAHEAD_DAYS}).",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"Days back to include for recent results (default: {DEFAULT_LOOKBACK_DAYS}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Prepare payloads without writing to MinIO or PostgreSQL.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    league_codes: tuple[str, ...]
    if args.league_code:
        if args.league_code not in ESPN_SLUGS:
            raise ValueError(
                f"League code '{args.league_code}' is not in ESPN_SLUGS. "
                f"Supported codes: {sorted(ESPN_SLUGS)}"
            )
        league_codes = (args.league_code,)
    else:
        league_codes = tuple(ESPN_SLUGS)

    run_id = str(uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    ingest_date = datetime.now(timezone.utc).date().isoformat()
    season_start = _current_season_start()
    bucket_name = os.getenv("MINIO_BUCKET", DEFAULT_BUCKET)

    prepared_files = []
    failed_leagues: list[str] = []

    for league_code in league_codes:
        print(f"Fetching {league_code} upcoming fixtures …")
        try:
            source_file = fetch_upcoming_fixtures(
                league_code=league_code,
                lookback_days=args.lookback_days,
                lookahead_days=args.lookahead_days,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"  ERROR fetching {league_code}: {exc}")
            failed_leagues.append(league_code)
            continue

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
                "source_file": source_file,
                "checksum": checksum,
                "row_count": row_count,
                "file_manifest": file_manifest,
            }
        )
        print(f"  {row_count} fixture rows for {league_code}.")

    if not prepared_files:
        print("ERROR: all leagues failed, aborting.")
        return 1

    checksums = [item["checksum"] for item in prepared_files]
    total_row_count = sum(item["row_count"] for item in prepared_files)
    completed_at = datetime.now(timezone.utc).isoformat()
    final_status = "prepared" if args.dry_run else ("failed" if failed_leagues else "completed")
    final_pipeline_run = build_pipeline_run(
        run_id=run_id,
        checksum=aggregate_checksum(checksums),
        row_count=total_row_count,
        started_at=started_at,
        completed_at=completed_at,
        status=final_status,
        error_message=(f"Failed leagues: {failed_leagues}" if failed_leagues else None),
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
            failed_run = build_pipeline_run(
                run_id=run_id,
                checksum=None,
                row_count=None,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc).isoformat(),
                status="failed",
                error_message=str(exc),
            )
            update_pipeline_run(conn, failed_run)
            raise
        finally:
            conn.close()

    payload = {
        "pipeline_run": final_pipeline_run.to_dict(),
        "files": [
            {
                "league_code": item["league_code"],
                "row_count": item["row_count"],
            }
            for item in prepared_files
        ],
        "file_manifests": [item["file_manifest"].to_dict() for item in prepared_files],
        "failed_leagues": failed_leagues,
        "dry_run": args.dry_run,
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
