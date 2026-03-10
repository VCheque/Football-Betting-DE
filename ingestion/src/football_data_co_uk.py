from __future__ import annotations

import csv
import hashlib
import io
from urllib.request import urlopen

from .config import BASE_URL, BRONZE_PREFIX, DEFAULT_BUCKET, ENTITY_NAME, SOURCE_NAME, League
from .models import FileManifest, PipelineRun, SourceFile


def season_code(season_start: int) -> str:
    start_code = str(season_start % 100).zfill(2)
    end_code = str((season_start + 1) % 100).zfill(2)
    return f"{start_code}{end_code}"


def season_label(season_start: int) -> str:
    return f"{season_start}/{season_start + 1}"


def build_source_url(league_code: str, season_start: int) -> str:
    return f"{BASE_URL}/{season_code(season_start)}/{league_code}.csv"


def fetch_source_file(league: League, season_start: int) -> SourceFile:
    url = build_source_url(league.code, season_start)
    with urlopen(url) as response:  # noqa: S310
        content = response.read()
    return SourceFile(
        source_url=url,
        file_name=f"{league.code}_{season_start}.csv",
        content_bytes=content,
    )


def compute_checksum(content_bytes: bytes) -> str:
    return hashlib.sha256(content_bytes).hexdigest()


def count_csv_rows(content_bytes: bytes) -> int:
    text_stream = io.StringIO(content_bytes.decode("utf-8", errors="replace"))
    reader = csv.reader(text_stream)
    total_rows = sum(1 for _ in reader)
    return max(total_rows - 1, 0)


def bronze_object_key(league_code: str, season_start: int, ingest_date: str, run_id: str, file_name: str) -> str:
    return (
        f"{BRONZE_PREFIX}/source={SOURCE_NAME}/entity={ENTITY_NAME}/"
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
        source_name=SOURCE_NAME,
        entity_name=ENTITY_NAME,
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
    bucket_name: str = DEFAULT_BUCKET,
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
