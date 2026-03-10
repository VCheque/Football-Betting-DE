from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import duckdb
import psycopg

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from ingestion.src.storage import (
    build_s3_client,
    ensure_bucket,
    local_silver_path,
    silver_object_key,
    upload_file,
)


OBJECT_KEY_PATTERN = (
    "bronze/source={source}/entity={entity}/ingest_date={ingest_date}/"
    "run_id={run_id}/league={league_code}/season={season_start}/{file_name}"
)

RAW_SOURCE_COLUMNS = (
    "Date",
    "HomeTeam",
    "AwayTeam",
    "FTR",
    "FTHG",
    "FTAG",
    "HTHG",
    "HTAG",
    "HS",
    "AS",
    "HST",
    "AST",
    "HF",
    "AF",
    "HC",
    "AC",
    "HY",
    "AY",
    "HR",
    "AR",
    "B365H",
    "B365D",
    "B365A",
    "PSH",
    "PSD",
    "PSA",
    "AvgH",
    "AvgD",
    "AvgA",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish silver_matches as a Parquet dataset in MinIO from the latest successful raw ingestion run."
    )
    parser.add_argument("--source-name", default="football_data_co_uk")
    parser.add_argument("--entity-name", default="matches_odds")
    parser.add_argument("--bucket-name", default=os.getenv("MINIO_BUCKET", "football"))
    parser.add_argument("--dremio-space", default="semantic")
    parser.add_argument("--dataset-name", default="silver_matches")
    parser.add_argument("--output-sql", default="dremio/sql/semantic_silver_matches.sql")
    return parser.parse_args()


def connect_postgres():
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "football_platform"),
        user=os.getenv("POSTGRES_USER", "football"),
        password=os.getenv("POSTGRES_PASSWORD", "football"),
        autocommit=True,
    )


def latest_successful_run(conn, source_name: str, entity_name: str) -> tuple[str, list[dict]]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT run_id
            FROM pipeline_run
            WHERE source_name = %s
              AND entity_name = %s
              AND status = 'completed'
            ORDER BY completed_at DESC NULLS LAST, started_at DESC
            LIMIT 1
            """,
            (source_name, entity_name),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"No successful pipeline_run found for {source_name}/{entity_name}")
        run_id = str(row["run_id"])

        cur.execute(
            """
            SELECT object_key
            FROM file_manifest
            WHERE run_id = %s
            ORDER BY object_key
            """,
            (run_id,),
        )
        manifests = cur.fetchall()
        if not manifests:
            raise RuntimeError(f"No file_manifest rows found for run_id={run_id}")
        return run_id, manifests


def parse_object_key(object_key: str) -> dict:
    prefix = "bronze/source="
    if not object_key.startswith(prefix):
        raise ValueError(f"Unsupported object_key format: {object_key}")

    parts = object_key.split("/")
    if len(parts) != 8:
        raise ValueError(f"Unsupported object_key structure: {object_key}")

    return {
        "source": parts[1].split("=", 1)[1],
        "entity": parts[2].split("=", 1)[1],
        "ingest_date": parts[3].split("=", 1)[1],
        "run_id": parts[4].split("=", 1)[1],
        "league_code": parts[5].split("=", 1)[1],
        "season_start": int(parts[6].split("=", 1)[1]),
        "file_name": parts[7],
    }


def season_label(season_start: int) -> str:
    return f"{season_start}/{season_start + 1}"


def sql_escape(value: str) -> str:
    return value.replace("'", "''")


def download_csvs(client, bucket_name: str, entries: list[dict], temp_dir: Path) -> list[dict]:
    downloaded = []
    for entry in entries:
        response = client.get_object(Bucket=bucket_name, Key=entry["object_key"])
        content = response["Body"].read().decode("utf-8-sig")
        local_path = temp_dir / entry["file_name"]
        local_path.write_text(content, encoding="utf-8")
        enriched = dict(entry)
        enriched["local_csv_path"] = local_path
        enriched["season_label"] = season_label(entry["season_start"])
        enriched["headers"] = read_csv_headers(local_path)
        downloaded.append(enriched)
    return downloaded


def read_csv_headers(local_path: Path) -> set[str]:
    with local_path.open("r", encoding="utf-8-sig", newline="") as file_handle:
        reader = csv.reader(file_handle)
        headers = next(reader, [])
    return {header.strip() for header in headers}


def select_expression(column_name: str, available_headers: set[str]) -> str:
    if column_name in available_headers:
        return f'"{column_name}" as "{column_name}"'
    return f"NULL as \"{column_name}\""


def build_union_sql(entries: list[dict]) -> str:
    unions = []
    for entry in entries:
        csv_path = sql_escape(str(entry["local_csv_path"]))
        stable_columns = ",\n    ".join(
            select_expression(column_name, entry["headers"]) for column_name in RAW_SOURCE_COLUMNS
        )
        unions.append(
            f"""select
    '{entry["season_label"]}' as Season,
    '{entry["run_id"]}' as RunId,
    '{entry["ingest_date"]}' as IngestDate,
    '{entry["league_code"]}' as LeagueCode,
    {stable_columns}
from read_csv_auto('{csv_path}', header=true, all_varchar=true)"""
        )
    return "\n\nunion all\n\n".join(unions)


def silver_transform_sql(entries: list[dict], output_path: Path) -> str:
    source_union = build_union_sql(entries)
    output = sql_escape(str(output_path))
    return f"""copy (
with raw_matches as (
{source_union}
)
select
    strptime("Date", '%d/%m/%Y')::date as match_date,
    cast("HomeTeam" as varchar) as home_team,
    cast("AwayTeam" as varchar) as away_team,
    cast("FTR" as varchar) as result_ft,
    cast("LeagueCode" as varchar) as league_code,
    cast("Season" as varchar) as season_label,
    cast("RunId" as varchar) as run_id,
    cast("IngestDate" as date) as ingest_date,
    cast("FTHG" as integer) as home_goals_ft,
    cast("FTAG" as integer) as away_goals_ft,
    cast("HTHG" as integer) as home_goals_ht,
    cast("HTAG" as integer) as away_goals_ht,
    cast("HS" as integer) as home_shots,
    cast("AS" as integer) as away_shots,
    cast("HST" as integer) as home_shots_on_target,
    cast("AST" as integer) as away_shots_on_target,
    cast("HF" as integer) as home_fouls,
    cast("AF" as integer) as away_fouls,
    cast("HC" as integer) as home_corners,
    cast("AC" as integer) as away_corners,
    cast("HY" as integer) as home_yellow_cards,
    cast("AY" as integer) as away_yellow_cards,
    cast("HR" as integer) as home_red_cards,
    cast("AR" as integer) as away_red_cards,
    cast("B365H" as double) as odds_b365_home,
    cast("B365D" as double) as odds_b365_draw,
    cast("B365A" as double) as odds_b365_away,
    cast("PSH" as double) as odds_pinnacle_home,
    cast("PSD" as double) as odds_pinnacle_draw,
    cast("PSA" as double) as odds_pinnacle_away,
    cast("AvgH" as double) as odds_avg_home,
    cast("AvgD" as double) as odds_avg_draw,
    cast("AvgA" as double) as odds_avg_away
from raw_matches
) to '{output}' (format parquet, compression zstd)"""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def dremio_request(method: str, url: str, token: str | None = None, payload: dict | None = None) -> dict | None:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = Request(url, data=data, headers=headers, method=method)
    with urlopen(request) as response:  # noqa: S310
        content = response.read()
        if not content:
            return None
        return json.loads(content)


def dremio_token() -> str:
    host = os.getenv("DREMIO_HOST", "localhost")
    port = os.getenv("DREMIO_PORT", "9047")
    user = os.getenv("DREMIO_USER", "admin")
    password = os.getenv("DREMIO_PASSWORD", "admin12345")
    response = dremio_request(
        "POST",
        f"http://{host}:{port}/apiv2/login",
        payload={"userName": user, "password": password},
    )
    assert response is not None
    return response["token"]


def dataset_by_path(token: str, dremio_space: str, dataset_name: str) -> dict | None:
    host = os.getenv("DREMIO_HOST", "localhost")
    port = os.getenv("DREMIO_PORT", "9047")
    try:
        return dremio_request(
            "GET",
            f"http://{host}:{port}/api/v3/catalog/by-path/{dremio_space}/{dataset_name}",
            token=token,
        )
    except HTTPError as exc:
        if exc.code == 404:
            return None
        raise


def delete_dataset(token: str, dataset_id: str) -> None:
    host = os.getenv("DREMIO_HOST", "localhost")
    port = os.getenv("DREMIO_PORT", "9047")
    dremio_request(
        "DELETE",
        f"http://{host}:{port}/api/v3/catalog/{dataset_id}",
        token=token,
    )


def create_virtual_dataset(token: str, dremio_space: str, dataset_name: str, sql_text: str) -> dict:
    host = os.getenv("DREMIO_HOST", "localhost")
    port = os.getenv("DREMIO_PORT", "9047")
    response = dremio_request(
        "POST",
        f"http://{host}:{port}/api/v3/catalog",
        token=token,
        payload={
            "entityType": "dataset",
            "type": "VIRTUAL_DATASET",
            "path": [dremio_space, dataset_name],
            "sql": sql_text,
            "sqlContext": [],
        },
    )
    assert response is not None
    return response


def build_semantic_sql(run_id: str, ingest_date: str, file_name: str = "silver_matches.parquet") -> str:
    return f"""-- Generated from the latest published Silver artifact.

select *
from TABLE(
    "bronze_minio"."football"."silver"."entity=matches"."dataset=silver_matches"."ingest_date={ingest_date}"."run_id={run_id}"."{file_name}"(
        type => 'parquet'
    )
)"""


def repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def write_sql_artifact(relative_path: str, sql_text: str) -> Path:
    target_path = repository_root() / relative_path
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(sql_text)
    return target_path


def main() -> int:
    args = parse_args()

    conn = connect_postgres()
    try:
        run_id, manifests = latest_successful_run(conn, args.source_name, args.entity_name)
    finally:
        conn.close()

    entries = []
    for row in manifests:
        entry = parse_object_key(row["object_key"])
        entry["object_key"] = row["object_key"]
        entries.append(entry)

    if not entries:
        raise RuntimeError("No raw manifests available to publish Silver")

    ingest_date = entries[0]["ingest_date"]
    silver_local_path = local_silver_path(run_id=run_id, ingest_date=ingest_date)
    silver_local_path.parent.mkdir(parents=True, exist_ok=True)

    s3_client = build_s3_client()
    ensure_bucket(s3_client, args.bucket_name)

    with tempfile.TemporaryDirectory() as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        downloaded_entries = download_csvs(s3_client, args.bucket_name, entries, temp_dir)
        sql = silver_transform_sql(downloaded_entries, silver_local_path)
        duckdb.sql(sql)

    silver_key = silver_object_key(run_id=run_id, ingest_date=ingest_date)
    checksum = sha256_file(silver_local_path)
    upload_file(
        client=s3_client,
        bucket_name=args.bucket_name,
        object_key=silver_key,
        local_path=silver_local_path,
        content_type="application/octet-stream",
        metadata={"sha256": checksum, "source_run_id": run_id},
    )

    semantic_sql = build_semantic_sql(run_id=run_id, ingest_date=ingest_date)
    output_sql_path = write_sql_artifact(args.output_sql, semantic_sql)

    token = dremio_token()
    existing_dataset = dataset_by_path(token, args.dremio_space, args.dataset_name)
    if existing_dataset is not None:
        delete_dataset(token, existing_dataset["id"])
    dataset = create_virtual_dataset(token, args.dremio_space, args.dataset_name, semantic_sql)

    print(
        json.dumps(
            {
                "run_id": run_id,
                "silver_object_key": silver_key,
                "local_silver_path": str(silver_local_path),
                "dataset_path": dataset["path"],
                "output_sql": str(output_sql_path),
                "checksum": checksum,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
