# src/performa_video/pipelines/config.py
"""Shared constants and mutable pipeline state for the Performa Video ETL."""

import os

from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = "database-sigma"

SRC_MINIO = {"system": "MinIO", "entity": "watermarks"}
TGT_BQ_BRONZE_VIDEO = {"system": "BigQuery", "entity": f"{PROJECT_ID}.BRONZE_DB.bronze_video"}
TGT_BQ_BRONZE_PRODUCTION = {"system": "BigQuery", "entity": f"{PROJECT_ID}.BRONZE_DB.bronze_video_production"}
TGT_BQ_SILVER_VIDEO = {"system": "BigQuery", "entity": "SILVER_DB.silver_tt_video"}
TGT_BQ_SILVER_PRODUCTION = {"system": "BigQuery", "entity": "SILVER_DB.silver_tt_video_production"}
TGT_GOLD = {"system": "BigQuery", "entity": "GOLD_DB.fact_video_performa_daily"}

WHITELIST_SHEETS = set(
    s.strip() for s in os.getenv("WHITELIST_SHEETS", "ian").split(",") if s.strip()
)

BQ_TARGETS = [
    {"table": TGT_BQ_BRONZE_VIDEO["entity"], "action": "append"},
    {"table": TGT_BQ_BRONZE_PRODUCTION["entity"], "action": "append"},
    {"table": TGT_BQ_SILVER_VIDEO["entity"], "action": "MERGE (silver upsert)"},
    {"table": TGT_BQ_SILVER_PRODUCTION["entity"], "action": "MERGE (silver upsert)"},
    {"table": TGT_GOLD["entity"], "action": "CREATE OR REPLACE (gold rebuild)"},
]

_failure_ctx = {
    "stage": "",
    "minio_files": [],
    "rollback_hint": "",
}
