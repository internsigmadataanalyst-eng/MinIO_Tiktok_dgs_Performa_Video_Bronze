# src/performa_video/pipelines/config.py
"""Shared constants and mutable pipeline state for the Performa Video ETL."""

import os

from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = "database-sigma"

SRC_GSHEET = {"system": "Google_Sheets", "entity": "video, produksi"}
SRC_MINIO = {"system": "MinIO", "entity": "watermarks"}
TGT_BQ_BRONZE_VIDEO = {"system": "BigQuery", "entity": f"{PROJECT_ID}.BRONZE_DB.bronze_video"}
TGT_BQ_BRONZE_PRODUCTION = {"system": "BigQuery", "entity": f"{PROJECT_ID}.BRONZE_DB.bronze_video_production"}
TGT_BQ_SILVER_VIDEO = {"system": "BigQuery", "entity": "SILVER_DB.silver_tt_video"}
TGT_BQ_SILVER_PRODUCTION = {"system": "BigQuery", "entity": "SILVER_DB.silver_tt_video_production"}
TGT_GOLD = {"system": "BigQuery", "entity": "GOLD_DB.fact_video_performa_daily"}

WHITELIST_SHEETS = set(
    s.strip() for s in os.getenv("WHITELIST_SHEETS", "ian").split(",") if s.strip()
)

# How long an open quarantine entry may keep the pre-flight gate open so PATH A
# can re-admit its repaired rows. Past this age the entry stops holding the gate
# open and is reported as STUCK in the gate-abort email - never silently
# forgotten. Only the video (gated) dataset can be held open by this; the
# produksi manifest is read for reporting but never gates.
QUARANTINE_RECOVERY_MAX_AGE_DAYS = int(
    os.getenv("QUARANTINE_RECOVERY_MAX_AGE_DAYS", "14")
)

# Per-table error-manifest paths. Declared here (not inline in run_daily_etl) so
# the pre-flight recovery check can read them BEFORE datasets_config is built -
# the gate runs before the GSheet fetch.
QUARANTINE_MANIFEST_PATHS = {
    "video": "error_list_watermark/video/error_manifest.json",
    "produksi": "error_list_watermark/produksi/error_manifest.json",
}

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
