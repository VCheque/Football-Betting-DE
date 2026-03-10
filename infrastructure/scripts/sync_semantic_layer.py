from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import psycopg


RAW_MATCH_COLUMNS = (
    "Div",
    "Date",
    "Time",
    "HomeTeam",
    "AwayTeam",
    "FTHG",
    "FTAG",
    "FTR",
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

OBJECT_KEY_PATTERN = re.compile(
    r"bronze/source=(?P<source>[^/]+)/entity=(?P<entity>[^/]+)/"
    r"ingest_date=(?P<ingest_date>\d{4}-\d{2}-\d{2})/run_id=(?P<run_id>[^/]+)/"
    r"league=(?P<league_code>[^/]+)/season=(?P<season_start>\d{4})/(?P<file_name>[^/]+)$"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate and publish a Dremio semantic raw view from the latest successful ingestion run."
    )
    parser.add_argument("--source-name", default="football_data_co_uk")
    parser.add_argument("--entity-name", default="matches_odds")
    parser.add_argument("--dremio-space", default="semantic")
    parser.add_argument("--dataset-name", default="raw_matches_odds")
    parser.add_argument(
        "--output-sql",
        default="dremio/sql/semantic_raw_matches_odds.sql",
        help="Path relative to the repository root for the generated SQL artifact.",
    )
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
    match = OBJECT_KEY_PATTERN.fullmatch(object_key)
    if match is None:
        raise ValueError(f"Unsupported object_key format: {object_key}")
    parsed = match.groupdict()
    parsed["season_start"] = int(parsed["season_start"])
    parsed["season_label"] = f"{parsed['season_start']}/{parsed['season_start'] + 1}"
    return parsed


def render_select(entry: dict) -> str:
    columns = ",\n    ".join(f'source_data."{column}"' for column in RAW_MATCH_COLUMNS)
    return f"""select
    {columns},
    '{entry["season_label"]}' as "Season",
    '{entry["run_id"]}' as "RunId",
    '{entry["ingest_date"]}' as "IngestDate",
    '{entry["league_code"]}' as "LeagueCode"
from TABLE(
    "bronze_minio"."football"."bronze"."source={entry["source"]}"."entity={entry["entity"]}"."ingest_date={entry["ingest_date"]}"."run_id={entry["run_id"]}"."league={entry["league_code"]}"."season={entry["season_start"]}"."{entry["file_name"]}"(
        type => 'text',
        fieldDelimiter => ',',
        extractHeader => true
    )
) as source_data"""


def build_raw_matches_sql(entries: list[dict]) -> str:
    header = (
        "-- Generated from PostgreSQL metadata.\n"
        "-- This file is rebuilt from the latest successful ingestion run.\n\n"
    )
    body = "\n\nunion all\n\n".join(render_select(entry) for entry in entries)
    return header + body + "\n"


def repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def write_sql_artifact(relative_path: str, sql_text: str) -> Path:
    target_path = repository_root() / relative_path
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(sql_text)
    return target_path


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


def main() -> int:
    args = parse_args()
    conn = connect_postgres()
    try:
        run_id, manifests = latest_successful_run(conn, args.source_name, args.entity_name)
    finally:
        conn.close()

    entries = [parse_object_key(row["object_key"]) for row in manifests]
    sql_text = build_raw_matches_sql(entries)
    output_path = write_sql_artifact(args.output_sql, sql_text)

    token = dremio_token()
    existing_dataset = dataset_by_path(token, args.dremio_space, args.dataset_name)
    if existing_dataset is not None:
        delete_dataset(token, existing_dataset["id"])
    dataset = create_virtual_dataset(token, args.dremio_space, args.dataset_name, sql_text)

    print(
        json.dumps(
            {
                "run_id": run_id,
                "dataset_path": dataset["path"],
                "output_sql": str(output_path),
                "manifest_count": len(entries),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
