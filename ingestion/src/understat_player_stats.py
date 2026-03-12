"""Understat player-stats fetcher.

Fetches per-player season statistics from understat.com for the five supported
leagues (Primeira Liga is NOT available on Understat) and serialises the result
as a CSV bytes payload ready to upload to MinIO Bronze.

Usage (from run_player_stats.py):
    from .understat_player_stats import (
        UNDERSTAT_LEAGUES,
        UNDERSTAT_SOURCE_NAME,
        UNDERSTAT_ENTITY_NAME,
        CSV_COLUMNS,
        fetch_player_stats,
        bronze_object_key,
        build_pipeline_run,
        build_file_manifest,
    )
"""

from __future__ import annotations

import asyncio
import csv
import io
from dataclasses import dataclass
from typing import Sequence

from .config import BRONZE_PREFIX, DEFAULT_BUCKET
from .football_data_co_uk import compute_checksum, count_csv_rows, season_label
from .models import FileManifest, PipelineRun

UNDERSTAT_SOURCE_NAME = "understat"
UNDERSTAT_ENTITY_NAME = "player_stats"

# football-data.co.uk league code → Understat league name
# Primeira Liga (P1) is intentionally excluded — not available on Understat.
UNDERSTAT_LEAGUES: dict[str, str] = {
    "E0":  "EPL",
    "SP1": "La_liga",
    "I1":  "Serie_A",
    "D1":  "Bundesliga",
    "F1":  "Ligue_1",
}

# CSV column order that matches the Dremio view and dbt staging model
CSV_COLUMNS: tuple[str, ...] = (
    "player_id",
    "player_name",
    "team",
    "position",
    "games",
    "minutes_played",
    "goals",
    "assists",
    "shots",
    "key_passes",
    "yellow_cards",
    "red_cards",
    "npg",
    "xg",
    "xa",
    "npxg",
    "season_label",
    "league_code",
)


@dataclass(frozen=True)
class UndPlayerSourceFile:
    source_url: str
    file_name: str
    content_bytes: bytes
    local_path: str = ""


async def _fetch_async(understat_league: str, season_start: int) -> list[dict]:
    """Download player stats from Understat via their internal POST API.

    Understat migrated to client-side rendering; the old HTML-embedded JSON
    approach no longer works. Their JS calls POST /main/getPlayersStats/ which
    returns {"success": true, "players": [...]} directly as JSON.
    """
    try:
        import aiohttp
    except ImportError as exc:
        raise ImportError("Install aiohttp: pip install aiohttp") from exc

    url = "https://understat.com/main/getPlayersStats/"
    payload = {"league": understat_league, "season": season_start}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=payload) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

    if not data.get("success"):
        raise ValueError(
            f"Understat API returned success=false for {understat_league}/{season_start}"
        )
    return data["players"]


def _to_csv_bytes(
    players: list[dict],
    league_code: str,
    season_start: int,
) -> bytes:
    """Serialize player records to CSV bytes with a fixed column order."""
    label = season_label(season_start)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for p in players:
        writer.writerow(
            {
                "player_id":    p.get("id", ""),
                "player_name":  p.get("player_name", ""),
                "team":         p.get("team_title", ""),
                "position":     p.get("position", ""),
                "games":        p.get("games", ""),
                "minutes_played": p.get("time", ""),
                "goals":        p.get("goals", ""),
                "assists":      p.get("assists", ""),
                "shots":        p.get("shots", ""),
                "key_passes":   p.get("key_passes", ""),
                "yellow_cards": p.get("yellow_cards", ""),
                "red_cards":    p.get("red_cards", ""),
                "npg":          p.get("npg", ""),
                "xg":           p.get("xG", ""),
                "xa":           p.get("xA", ""),
                "npxg":         p.get("npxG", ""),
                "season_label": label,
                "league_code":  league_code,
            }
        )
    return buf.getvalue().encode("utf-8")


def fetch_player_stats(
    league_code: str,
    season_start: int,
) -> UndPlayerSourceFile:
    """Fetch player stats for one league + season and return as CSV bytes."""
    understat_league = UNDERSTAT_LEAGUES[league_code]
    players = asyncio.run(_fetch_async(understat_league, season_start))
    content_bytes = _to_csv_bytes(players, league_code, season_start)
    file_name = f"{league_code}_{season_start}_players.csv"
    source_url = f"https://understat.com/league/{understat_league}/{season_start}"
    return UndPlayerSourceFile(
        source_url=source_url,
        file_name=file_name,
        content_bytes=content_bytes,
    )


def bronze_object_key(
    league_code: str,
    season_start: int,
    ingest_date: str,
    run_id: str,
    file_name: str,
) -> str:
    return (
        f"{BRONZE_PREFIX}/source={UNDERSTAT_SOURCE_NAME}/entity={UNDERSTAT_ENTITY_NAME}/"
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
        source_name=UNDERSTAT_SOURCE_NAME,
        entity_name=UNDERSTAT_ENTITY_NAME,
        status=status,
        row_count=row_count,
        checksum=checksum,
        started_at=started_at,
        completed_at=completed_at,
        error_message=error_message,
    )


def build_file_manifest(
    run_id: str,
    source_file: UndPlayerSourceFile,
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
