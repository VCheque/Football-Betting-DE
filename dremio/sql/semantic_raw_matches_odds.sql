-- Generated from PostgreSQL metadata.
-- This file is rebuilt from the latest successful ingestion run.

select
    source_data."Div",
    source_data."Date",
    source_data."Time",
    source_data."HomeTeam",
    source_data."AwayTeam",
    source_data."FTHG",
    source_data."FTAG",
    source_data."FTR",
    source_data."HTHG",
    source_data."HTAG",
    source_data."HS",
    source_data."AS",
    source_data."HST",
    source_data."AST",
    source_data."HF",
    source_data."AF",
    source_data."HC",
    source_data."AC",
    source_data."HY",
    source_data."AY",
    source_data."HR",
    source_data."AR",
    source_data."B365H",
    source_data."B365D",
    source_data."B365A",
    source_data."PSH",
    source_data."PSD",
    source_data."PSA",
    source_data."AvgH",
    source_data."AvgD",
    source_data."AvgA",
    '2024/2025' as "Season",
    'eb3006c2-8365-4581-9005-debf228c148e' as "RunId",
    '2026-03-10' as "IngestDate",
    'D1' as "LeagueCode"
from TABLE(
    "bronze_minio"."football"."bronze"."source=football_data_co_uk"."entity=matches_odds"."ingest_date=2026-03-10"."run_id=eb3006c2-8365-4581-9005-debf228c148e"."league=D1"."season=2024"."D1_2024.csv"(
        type => 'text',
        fieldDelimiter => ',',
        extractHeader => true
    )
) as source_data

union all

select
    source_data."Div",
    source_data."Date",
    source_data."Time",
    source_data."HomeTeam",
    source_data."AwayTeam",
    source_data."FTHG",
    source_data."FTAG",
    source_data."FTR",
    source_data."HTHG",
    source_data."HTAG",
    source_data."HS",
    source_data."AS",
    source_data."HST",
    source_data."AST",
    source_data."HF",
    source_data."AF",
    source_data."HC",
    source_data."AC",
    source_data."HY",
    source_data."AY",
    source_data."HR",
    source_data."AR",
    source_data."B365H",
    source_data."B365D",
    source_data."B365A",
    source_data."PSH",
    source_data."PSD",
    source_data."PSA",
    source_data."AvgH",
    source_data."AvgD",
    source_data."AvgA",
    '2025/2026' as "Season",
    'eb3006c2-8365-4581-9005-debf228c148e' as "RunId",
    '2026-03-10' as "IngestDate",
    'D1' as "LeagueCode"
from TABLE(
    "bronze_minio"."football"."bronze"."source=football_data_co_uk"."entity=matches_odds"."ingest_date=2026-03-10"."run_id=eb3006c2-8365-4581-9005-debf228c148e"."league=D1"."season=2025"."D1_2025.csv"(
        type => 'text',
        fieldDelimiter => ',',
        extractHeader => true
    )
) as source_data

union all

select
    source_data."Div",
    source_data."Date",
    source_data."Time",
    source_data."HomeTeam",
    source_data."AwayTeam",
    source_data."FTHG",
    source_data."FTAG",
    source_data."FTR",
    source_data."HTHG",
    source_data."HTAG",
    source_data."HS",
    source_data."AS",
    source_data."HST",
    source_data."AST",
    source_data."HF",
    source_data."AF",
    source_data."HC",
    source_data."AC",
    source_data."HY",
    source_data."AY",
    source_data."HR",
    source_data."AR",
    source_data."B365H",
    source_data."B365D",
    source_data."B365A",
    source_data."PSH",
    source_data."PSD",
    source_data."PSA",
    source_data."AvgH",
    source_data."AvgD",
    source_data."AvgA",
    '2024/2025' as "Season",
    'eb3006c2-8365-4581-9005-debf228c148e' as "RunId",
    '2026-03-10' as "IngestDate",
    'E0' as "LeagueCode"
from TABLE(
    "bronze_minio"."football"."bronze"."source=football_data_co_uk"."entity=matches_odds"."ingest_date=2026-03-10"."run_id=eb3006c2-8365-4581-9005-debf228c148e"."league=E0"."season=2024"."E0_2024.csv"(
        type => 'text',
        fieldDelimiter => ',',
        extractHeader => true
    )
) as source_data

union all

select
    source_data."Div",
    source_data."Date",
    source_data."Time",
    source_data."HomeTeam",
    source_data."AwayTeam",
    source_data."FTHG",
    source_data."FTAG",
    source_data."FTR",
    source_data."HTHG",
    source_data."HTAG",
    source_data."HS",
    source_data."AS",
    source_data."HST",
    source_data."AST",
    source_data."HF",
    source_data."AF",
    source_data."HC",
    source_data."AC",
    source_data."HY",
    source_data."AY",
    source_data."HR",
    source_data."AR",
    source_data."B365H",
    source_data."B365D",
    source_data."B365A",
    source_data."PSH",
    source_data."PSD",
    source_data."PSA",
    source_data."AvgH",
    source_data."AvgD",
    source_data."AvgA",
    '2025/2026' as "Season",
    'eb3006c2-8365-4581-9005-debf228c148e' as "RunId",
    '2026-03-10' as "IngestDate",
    'E0' as "LeagueCode"
from TABLE(
    "bronze_minio"."football"."bronze"."source=football_data_co_uk"."entity=matches_odds"."ingest_date=2026-03-10"."run_id=eb3006c2-8365-4581-9005-debf228c148e"."league=E0"."season=2025"."E0_2025.csv"(
        type => 'text',
        fieldDelimiter => ',',
        extractHeader => true
    )
) as source_data

union all

select
    source_data."Div",
    source_data."Date",
    source_data."Time",
    source_data."HomeTeam",
    source_data."AwayTeam",
    source_data."FTHG",
    source_data."FTAG",
    source_data."FTR",
    source_data."HTHG",
    source_data."HTAG",
    source_data."HS",
    source_data."AS",
    source_data."HST",
    source_data."AST",
    source_data."HF",
    source_data."AF",
    source_data."HC",
    source_data."AC",
    source_data."HY",
    source_data."AY",
    source_data."HR",
    source_data."AR",
    source_data."B365H",
    source_data."B365D",
    source_data."B365A",
    source_data."PSH",
    source_data."PSD",
    source_data."PSA",
    source_data."AvgH",
    source_data."AvgD",
    source_data."AvgA",
    '2024/2025' as "Season",
    'eb3006c2-8365-4581-9005-debf228c148e' as "RunId",
    '2026-03-10' as "IngestDate",
    'F1' as "LeagueCode"
from TABLE(
    "bronze_minio"."football"."bronze"."source=football_data_co_uk"."entity=matches_odds"."ingest_date=2026-03-10"."run_id=eb3006c2-8365-4581-9005-debf228c148e"."league=F1"."season=2024"."F1_2024.csv"(
        type => 'text',
        fieldDelimiter => ',',
        extractHeader => true
    )
) as source_data

union all

select
    source_data."Div",
    source_data."Date",
    source_data."Time",
    source_data."HomeTeam",
    source_data."AwayTeam",
    source_data."FTHG",
    source_data."FTAG",
    source_data."FTR",
    source_data."HTHG",
    source_data."HTAG",
    source_data."HS",
    source_data."AS",
    source_data."HST",
    source_data."AST",
    source_data."HF",
    source_data."AF",
    source_data."HC",
    source_data."AC",
    source_data."HY",
    source_data."AY",
    source_data."HR",
    source_data."AR",
    source_data."B365H",
    source_data."B365D",
    source_data."B365A",
    source_data."PSH",
    source_data."PSD",
    source_data."PSA",
    source_data."AvgH",
    source_data."AvgD",
    source_data."AvgA",
    '2025/2026' as "Season",
    'eb3006c2-8365-4581-9005-debf228c148e' as "RunId",
    '2026-03-10' as "IngestDate",
    'F1' as "LeagueCode"
from TABLE(
    "bronze_minio"."football"."bronze"."source=football_data_co_uk"."entity=matches_odds"."ingest_date=2026-03-10"."run_id=eb3006c2-8365-4581-9005-debf228c148e"."league=F1"."season=2025"."F1_2025.csv"(
        type => 'text',
        fieldDelimiter => ',',
        extractHeader => true
    )
) as source_data

union all

select
    source_data."Div",
    source_data."Date",
    source_data."Time",
    source_data."HomeTeam",
    source_data."AwayTeam",
    source_data."FTHG",
    source_data."FTAG",
    source_data."FTR",
    source_data."HTHG",
    source_data."HTAG",
    source_data."HS",
    source_data."AS",
    source_data."HST",
    source_data."AST",
    source_data."HF",
    source_data."AF",
    source_data."HC",
    source_data."AC",
    source_data."HY",
    source_data."AY",
    source_data."HR",
    source_data."AR",
    source_data."B365H",
    source_data."B365D",
    source_data."B365A",
    source_data."PSH",
    source_data."PSD",
    source_data."PSA",
    source_data."AvgH",
    source_data."AvgD",
    source_data."AvgA",
    '2024/2025' as "Season",
    'eb3006c2-8365-4581-9005-debf228c148e' as "RunId",
    '2026-03-10' as "IngestDate",
    'I1' as "LeagueCode"
from TABLE(
    "bronze_minio"."football"."bronze"."source=football_data_co_uk"."entity=matches_odds"."ingest_date=2026-03-10"."run_id=eb3006c2-8365-4581-9005-debf228c148e"."league=I1"."season=2024"."I1_2024.csv"(
        type => 'text',
        fieldDelimiter => ',',
        extractHeader => true
    )
) as source_data

union all

select
    source_data."Div",
    source_data."Date",
    source_data."Time",
    source_data."HomeTeam",
    source_data."AwayTeam",
    source_data."FTHG",
    source_data."FTAG",
    source_data."FTR",
    source_data."HTHG",
    source_data."HTAG",
    source_data."HS",
    source_data."AS",
    source_data."HST",
    source_data."AST",
    source_data."HF",
    source_data."AF",
    source_data."HC",
    source_data."AC",
    source_data."HY",
    source_data."AY",
    source_data."HR",
    source_data."AR",
    source_data."B365H",
    source_data."B365D",
    source_data."B365A",
    source_data."PSH",
    source_data."PSD",
    source_data."PSA",
    source_data."AvgH",
    source_data."AvgD",
    source_data."AvgA",
    '2025/2026' as "Season",
    'eb3006c2-8365-4581-9005-debf228c148e' as "RunId",
    '2026-03-10' as "IngestDate",
    'I1' as "LeagueCode"
from TABLE(
    "bronze_minio"."football"."bronze"."source=football_data_co_uk"."entity=matches_odds"."ingest_date=2026-03-10"."run_id=eb3006c2-8365-4581-9005-debf228c148e"."league=I1"."season=2025"."I1_2025.csv"(
        type => 'text',
        fieldDelimiter => ',',
        extractHeader => true
    )
) as source_data

union all

select
    source_data."Div",
    source_data."Date",
    source_data."Time",
    source_data."HomeTeam",
    source_data."AwayTeam",
    source_data."FTHG",
    source_data."FTAG",
    source_data."FTR",
    source_data."HTHG",
    source_data."HTAG",
    source_data."HS",
    source_data."AS",
    source_data."HST",
    source_data."AST",
    source_data."HF",
    source_data."AF",
    source_data."HC",
    source_data."AC",
    source_data."HY",
    source_data."AY",
    source_data."HR",
    source_data."AR",
    source_data."B365H",
    source_data."B365D",
    source_data."B365A",
    source_data."PSH",
    source_data."PSD",
    source_data."PSA",
    source_data."AvgH",
    source_data."AvgD",
    source_data."AvgA",
    '2024/2025' as "Season",
    'eb3006c2-8365-4581-9005-debf228c148e' as "RunId",
    '2026-03-10' as "IngestDate",
    'P1' as "LeagueCode"
from TABLE(
    "bronze_minio"."football"."bronze"."source=football_data_co_uk"."entity=matches_odds"."ingest_date=2026-03-10"."run_id=eb3006c2-8365-4581-9005-debf228c148e"."league=P1"."season=2024"."P1_2024.csv"(
        type => 'text',
        fieldDelimiter => ',',
        extractHeader => true
    )
) as source_data

union all

select
    source_data."Div",
    source_data."Date",
    source_data."Time",
    source_data."HomeTeam",
    source_data."AwayTeam",
    source_data."FTHG",
    source_data."FTAG",
    source_data."FTR",
    source_data."HTHG",
    source_data."HTAG",
    source_data."HS",
    source_data."AS",
    source_data."HST",
    source_data."AST",
    source_data."HF",
    source_data."AF",
    source_data."HC",
    source_data."AC",
    source_data."HY",
    source_data."AY",
    source_data."HR",
    source_data."AR",
    source_data."B365H",
    source_data."B365D",
    source_data."B365A",
    source_data."PSH",
    source_data."PSD",
    source_data."PSA",
    source_data."AvgH",
    source_data."AvgD",
    source_data."AvgA",
    '2025/2026' as "Season",
    'eb3006c2-8365-4581-9005-debf228c148e' as "RunId",
    '2026-03-10' as "IngestDate",
    'P1' as "LeagueCode"
from TABLE(
    "bronze_minio"."football"."bronze"."source=football_data_co_uk"."entity=matches_odds"."ingest_date=2026-03-10"."run_id=eb3006c2-8365-4581-9005-debf228c148e"."league=P1"."season=2025"."P1_2025.csv"(
        type => 'text',
        fieldDelimiter => ',',
        extractHeader => true
    )
) as source_data

union all

select
    source_data."Div",
    source_data."Date",
    source_data."Time",
    source_data."HomeTeam",
    source_data."AwayTeam",
    source_data."FTHG",
    source_data."FTAG",
    source_data."FTR",
    source_data."HTHG",
    source_data."HTAG",
    source_data."HS",
    source_data."AS",
    source_data."HST",
    source_data."AST",
    source_data."HF",
    source_data."AF",
    source_data."HC",
    source_data."AC",
    source_data."HY",
    source_data."AY",
    source_data."HR",
    source_data."AR",
    source_data."B365H",
    source_data."B365D",
    source_data."B365A",
    source_data."PSH",
    source_data."PSD",
    source_data."PSA",
    source_data."AvgH",
    source_data."AvgD",
    source_data."AvgA",
    '2024/2025' as "Season",
    'eb3006c2-8365-4581-9005-debf228c148e' as "RunId",
    '2026-03-10' as "IngestDate",
    'SP1' as "LeagueCode"
from TABLE(
    "bronze_minio"."football"."bronze"."source=football_data_co_uk"."entity=matches_odds"."ingest_date=2026-03-10"."run_id=eb3006c2-8365-4581-9005-debf228c148e"."league=SP1"."season=2024"."SP1_2024.csv"(
        type => 'text',
        fieldDelimiter => ',',
        extractHeader => true
    )
) as source_data

union all

select
    source_data."Div",
    source_data."Date",
    source_data."Time",
    source_data."HomeTeam",
    source_data."AwayTeam",
    source_data."FTHG",
    source_data."FTAG",
    source_data."FTR",
    source_data."HTHG",
    source_data."HTAG",
    source_data."HS",
    source_data."AS",
    source_data."HST",
    source_data."AST",
    source_data."HF",
    source_data."AF",
    source_data."HC",
    source_data."AC",
    source_data."HY",
    source_data."AY",
    source_data."HR",
    source_data."AR",
    source_data."B365H",
    source_data."B365D",
    source_data."B365A",
    source_data."PSH",
    source_data."PSD",
    source_data."PSA",
    source_data."AvgH",
    source_data."AvgD",
    source_data."AvgA",
    '2025/2026' as "Season",
    'eb3006c2-8365-4581-9005-debf228c148e' as "RunId",
    '2026-03-10' as "IngestDate",
    'SP1' as "LeagueCode"
from TABLE(
    "bronze_minio"."football"."bronze"."source=football_data_co_uk"."entity=matches_odds"."ingest_date=2026-03-10"."run_id=eb3006c2-8365-4581-9005-debf228c148e"."league=SP1"."season=2025"."SP1_2025.csv"(
        type => 'text',
        fieldDelimiter => ',',
        extractHeader => true
    )
) as source_data
