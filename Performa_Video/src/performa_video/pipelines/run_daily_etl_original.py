# src/performa_video/pipelines/run_daily_etl.py

import os
from google.oauth2 import service_account

from performa_video.utils.gsheet_client import get_gspread_client
from performa_video.utils.bq_client import get_bq_client
from performa_video.ingestion.fetch_performa_video_gsheet import (
    fetch_tiktok_video,
    fetch_tiktok_produksi
)
from performa_video.transform.clean_bronze import build_bronze_video
from performa_video.transform.merge_silver import merge_to_silver
from performa_video.transform.build_gold import build_fact_performa_video
from performa_video.load.load_to_bigquery import load_df

PROJECT_ID = "database-sigma"


def _get_credentials():
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path:
        raise RuntimeError("Env GOOGLE_APPLICATION_CREDENTIALS belum di-set")
    return service_account.Credentials.from_service_account_file(sa_path)


def run_daily_etl():
    print("== Start ETL Performa Video ==")

    # 1) Client
    gc = get_gspread_client()
    bq_client = get_bq_client()
    creds = _get_credentials()

    # 2) Ingest dari GSheet
    df_tt_vid_raw = fetch_tiktok_video(gc)
    print(f"[INGEST] Rows raw from GSheet: {len(df_tt_vid_raw)}")

    # 3) Bronze: cleaning + snapshot + hash
    df_bronze, _ = build_bronze_video(df_tt_vid_raw)
    print(f"[BRONZE] Rows bronze to load: {len(df_bronze)}")

    load_df(
        df_bronze,
        table_id="BRONZE_DB.bronze_video",
        project_id=PROJECT_ID,
        if_exists="append",
        credentials=creds,
    )
    print("[BRONZE] Load to BRONZE_DB.bronze_video DONE")

    # 4) Silver: MERGE
    print("[SILVER] Running MERGE into SILVER_DB.silver_tt_video ...")
    merge_to_silver()
    print("[SILVER] MERGE DONE")

    # 5) Gold: fact_performa_video_daily
    print("[GOLD] Building fact_performa_video_daily ...")
    df_fact = build_fact_performa_video(bq_client)
    print(f"[GOLD] Rows fact_performa_video_daily: {len(df_fact)}")

    load_df(
        df_fact,
        table_id="GOLD_DB.fact_video_performa_daily",
        project_id=PROJECT_ID,
        if_exists="replace",  # nanti bisa jadi MERGE kalau mau incremental
        credentials=creds,
    )
    print("[GOLD] Load to GOLD_DB.fact_video_performa_daily DONE")

    print("== ETL Performa Video DONE ==")

# Kalau kamu mau bisa juga di-run langsung:
if __name__ == "__main__":
    run_daily_etl()