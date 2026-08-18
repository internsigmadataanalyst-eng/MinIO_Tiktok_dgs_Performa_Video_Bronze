# src/performa_video/pipelines/run_daily_etl.py

import io
import os
from datetime import date

from dotenv import load_dotenv
from google.oauth2 import service_account

# Load variables from .env into environment
load_dotenv()

from src.performa_video.ingestion.fetch_performa_video_gsheet import (
    fetch_tiktok_produksi,
    fetch_tiktok_video,
    SHEET_REGISTRY,
)
from src.performa_video.transform.clean_bronze import (
    build_bronze_produksi,
    build_bronze_video,
)
from src.performa_video.utils.bq_client import get_bq_client
from src.performa_video.utils.gsheet_client import get_gspread_client
from src.performa_video.utils.minio_client import (
    get_minio_client,
    get_sheet_watermarks,
    update_sheet_watermarks,
    write_quarantine,
    sync_error_manifest,
)
from src.performa_video.utils.transform_utils import (
    NUMERIC_COLS,
    PERCENT_COLS,
    to_snake_case,
    validate_and_normalize_raw,
)

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
    minio_client, minio_bucket = get_minio_client()

    # 2) Date keys & Cutoff
    #    partition pakai YYYYMMDD, nama file pakai YYYYMMDDHH
    #    (jam agar 2 run di hari yang sama menghasilkan file terpisah, tanpa overwrite).
    today_obj = date.today()
    today_key = today_obj.strftime("%Y%m%d")
    run_key = today_obj.strftime("%Y%m%d%H")

    # 3) Ingest dari GSheet (tiap sheet di-tag sheet_name)
    df_tt_vid_raw = fetch_tiktok_video(gc)
    df_tt_prod_raw = fetch_tiktok_produksi(gc)
    print(f"[INGEST] Rows raw video from GSheet: {len(df_tt_vid_raw)}")
    print(f"[INGEST] Rows raw produksi from GSheet: {len(df_tt_prod_raw)}")

    # 4) Definisikan konfigurasi tiap dataset (build fn, raw df, path file & path watermark)
    datasets_config = {
        "video": {
            "build_fn": build_bronze_video,
            "raw_df": df_tt_vid_raw,
            "id_col": "tanggal",
            "date_col": "tanggal",
            "numeric_cols": NUMERIC_COLS,
            "percent_cols": PERCENT_COLS,
            "file_path": f"performa/video/date={today_key}/video_{run_key}.parquet",
            "watermark_path": "watermarks/performa_video.json",
            "manifest_path": "error_list_watermark/video/error_manifest.json",
            "fix_prefix": "fix_error_list_watermark/video",
        },
        "produksi": {
            "build_fn": build_bronze_produksi,
            "raw_df": df_tt_prod_raw,
            "id_col": "id_konten",
            "date_col": "tanggal",
            "numeric_cols": [],
            "percent_cols": None,
            "file_path": f"produksi/date={today_key}/produksi_{run_key}.parquet",
            "watermark_path": "watermarks/produksi.json",
            "manifest_path": "error_list_watermark/produksi/error_manifest.json",
            "fix_prefix": "fix_error_list_watermark/produksi",
        },
    }

    # 5) Loop pemrosesan per-sheet incremental & upload terpisah untuk setiap dataset
    #    Resolve SHEET_REGISTRY (name -> env key) ke nilai creds (spreadsheet ID) sekali,
    #    supaya FAILSAFE migrasi format lama (sheet_name -> creds) memetakan ke ID yang benar.
    sheet_registry = {name: os.getenv(env_key) for name, env_key in SHEET_REGISTRY.items()}
    sheet_registry["produksi"] = os.getenv("SH_KEY_PRODUKSI")
    for name, cfg in datasets_config.items():
        print(f"\n--- Processing dataset: {name.upper()} ---")
        watermark_path = cfg["watermark_path"]
        file_path = cfg["file_path"]

        # STEP 2: validate & normalize as early as possible (mixed-column
        #     detection + date-error capture). Runs exactly once, before anything else.

        # buang baris tanpa id
        key_col = next(
            c for c in cfg["raw_df"].columns if to_snake_case(str(c)) == cfg["id_col"]
        )
        df_raw = cfg["raw_df"]
        df_raw = df_raw[df_raw[key_col].astype(str).str.strip() != ""]

        date_col = next(
            c for c in df_raw.columns if to_snake_case(str(c)) == cfg["date_col"]
        )
        df_valid, df_error, v_report = validate_and_normalize_raw(
            df_raw, cfg["numeric_cols"], percent_cols=cfg["percent_cols"], date_col=date_col
        )
        print(
            f"[VALIDATE] Rows valid: {len(df_valid)} | bad rows: {v_report['n_bad_rows']} "
            f"(date errors: {v_report['n_date_errors']}) | blank rows dropped: {v_report['n_blank_rows']}"
        )
        if v_report["has_changes"]:
            print(f"[VALIDATE] Corrupted/Shifted columns: {v_report['affected_columns']}")
            print(
                f"[VALIDATE] Affected date range: {v_report['first_affected_date']} "
                f"---> {v_report['last_affected_date']}"
            )

        # STEP 3Q/6: sync error manifest EVERY run (append new open entries +
        # resolve entries whose format has been fixed since the last run).
        sync_error_manifest(minio_client, minio_bucket, df_error, v_report, today_key, run_key, subfolder=name, manifest_path=cfg["manifest_path"], fix_prefix=cfg["fix_prefix"], date_col=date_col)

        if not df_error.empty:
            write_quarantine(minio_client, minio_bucket, df_error, today_key, run_key, subfolder=name)

        # Get per-sheet watermark spesifik untuk dataset ini
        # sheet_registry hanya dibutuhkan utk FAILSAFE migrasi format lama (sheet_name -> creds).
        watermark_map, watermark_records = get_sheet_watermarks(
            minio_client, minio_bucket, watermark_path, sheet_registry=sheet_registry
        )

        # Bronze: cleaning + per-sheet incremental filter
        df_filtered, sheet_max_dates = cfg["build_fn"](
            df_valid, sheet_watermarks=watermark_map
        )
        print(f"[BRONZE] Rows bronze {name} to load: {len(df_filtered)}")

        if df_filtered.empty:
            print(f"[MINIO] Skip uploading {name}, no new rows available to process.")
            continue

        # Folder partition marker
        folder_path = f"{file_path.rsplit('/', 1)[0]}/"
        minio_client.put_object(minio_bucket, folder_path, io.BytesIO(b""), length=0)

        # Upload parquet ke MinIO
        parquet_bytes = df_filtered.to_parquet(index=False, engine="pyarrow")
        minio_client.put_object(
            minio_bucket,
            file_path,
            io.BytesIO(parquet_bytes),
            length=len(parquet_bytes),
            content_type="application/octet-stream",
        )
        print(f"[MINIO] Successfully Loaded {name} to: {file_path}")

        # Update per-sheet watermark masing-masing dataset (selalu tulis format baru)
        update_sheet_watermarks(
            minio_client, minio_bucket, watermark_path, watermark_records, sheet_max_dates,
            sheet_registry=sheet_registry,
        )

    print("\n== ETL Performa Video DONE ==")


if __name__ == "__main__":
    run_daily_etl()
