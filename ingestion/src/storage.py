from __future__ import annotations

import os
from pathlib import Path

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from .config import ENTITY_NAME, LOCAL_RAW_ROOT, LOCAL_SILVER_ROOT, SILVER_PREFIX, SOURCE_NAME


def build_s3_client() -> BaseClient:
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


def ensure_bucket(client: BaseClient, bucket_name: str) -> None:
    try:
        client.head_bucket(Bucket=bucket_name)
    except ClientError:
        client.create_bucket(Bucket=bucket_name)


def upload_raw_file(client: BaseClient, bucket_name: str, object_key: str, content_bytes: bytes, checksum: str) -> None:
    client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=content_bytes,
        ContentType="text/csv",
        Metadata={"sha256": checksum},
    )


def local_raw_path(run_id: str, ingest_date: str, league_code: str, season_start: int, file_name: str) -> Path:
    root = Path(os.getenv("LOCAL_RAW_ROOT", str(LOCAL_RAW_ROOT)))
    return (
        root
        / f"source={SOURCE_NAME}"
        / f"entity={ENTITY_NAME}"
        / f"ingest_date={ingest_date}"
        / f"run_id={run_id}"
        / f"league={league_code}"
        / f"season={season_start}"
        / file_name
    )


def write_local_raw_file(run_id: str, ingest_date: str, league_code: str, season_start: int, file_name: str, content_bytes: bytes) -> Path:
    target_path = local_raw_path(
        run_id=run_id,
        ingest_date=ingest_date,
        league_code=league_code,
        season_start=season_start,
        file_name=file_name,
    )
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(content_bytes)
    return target_path


def silver_object_key(run_id: str, ingest_date: str, file_name: str = "silver_matches.parquet") -> str:
    return (
        f"{SILVER_PREFIX}/entity=matches/dataset=silver_matches/"
        f"ingest_date={ingest_date}/run_id={run_id}/{file_name}"
    )


def local_silver_path(run_id: str, ingest_date: str, file_name: str = "silver_matches.parquet") -> Path:
    root = Path(os.getenv("LOCAL_SILVER_ROOT", str(LOCAL_SILVER_ROOT)))
    return (
        root
        / "entity=matches"
        / "dataset=silver_matches"
        / f"ingest_date={ingest_date}"
        / f"run_id={run_id}"
        / file_name
    )


def upload_file(client: BaseClient, bucket_name: str, object_key: str, local_path: Path, content_type: str, metadata: dict[str, str] | None = None) -> None:
    with local_path.open("rb") as file_handle:
        client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=file_handle,
            ContentType=content_type,
            Metadata=metadata or {},
        )
