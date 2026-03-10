-- Generated from the latest published Silver artifact.

select *
from TABLE(
    "bronze_minio"."football"."silver"."entity=matches"."dataset=silver_matches"."ingest_date=2026-03-10"."run_id=06e6bf4e-a881-4ff6-a75f-6d4516b4aaf1"."silver_matches.parquet"(
        type => 'parquet'
    )
)