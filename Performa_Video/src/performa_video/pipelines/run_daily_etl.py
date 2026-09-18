# src/performa_video/pipelines/run_daily_etl.py

import io
import os
import traceback
from datetime import datetime

import pandas as pd

from dotenv import load_dotenv
from google.oauth2 import service_account

# Load variables from .env into environment
load_dotenv()

from src.performa_video.ingestion.fetch_performa_video_gsheet import (
    fetch_tiktok_produksi,
    fetch_tiktok_video,
    SHEET_REGISTRY,
)
from src.performa_video.load.load_to_bigquery import load_df
from src.performa_video.transform.clean_bronze import (
    build_bronze_produksi,
    build_bronze_video,
)

from src.performa_video.transform.merge_silver import (
    merge_to_silver_video,
    merge_to_silver_production
)
from src.performa_video.transform.create_gold import create_gold_fact_video_performa_daily

from src.performa_video.utils.gsheet_client import get_gspread_client, init_spreadsheet_objects
from src.performa_video.utils.minio_client import (
    get_minio_client,
    get_sheet_watermarks,
    update_sheet_watermarks,
    filter_already_quarantined,
    write_quarantine,
    sync_error_manifest,
    QUARANTINE_PREFIX,
)
from src.performa_video.utils.transform_utils import (
    NUMERIC_COLS,
    PERCENT_COLS,
    to_snake_case,
    validate_and_normalize_raw,
)
from src.performa_video.utils.bronze_compare import (
    show_watermark,
    effective_watermark_changes,
)
from src.performa_video.utils.watermark_monitor import (
    performa_video_watermark_check,
    produksi_watermark_check,
)
from src.performa_video.utils.log import (
    get_log_folder,
    write_section_log,
    setup_event_logging,
    is_event_logging_enabled,
    emit,
)
from src.performa_video.utils.notify import (
    send_alert_email,
    build_gate_abort_email,
    build_pipeline_success_email,
    build_quarantine_email,
    build_recovery_email,
    QUARANTINE_SAMPLE_ROWS,
    QUARANTINE_SAMPLE_COLUMNS,
)

PROJECT_ID = "database-sigma"

SRC_MINIO = {"system": "MinIO", "entity": "watermarks"}
TGT_BQ_BRONZE_VIDEO = {"system": "BigQuery", "entity": f"{PROJECT_ID}.BRONZE_DB.bronze_video"}
TGT_BQ_BRONZE_PRODUCTION = {"system": "BigQuery", "entity": f"{PROJECT_ID}.BRONZE_DB.bronze_video_production"}
TGT_BQ_SILVER_VIDEO = {"system": "BigQuery", "entity": "SILVER_DB.silver_tt_video"}
TGT_BQ_SILVER_PRODUCTION = {"system": "BigQuery", "entity": "SILVER_DB.silver_tt_video_production"}
TGT_GOLD = {"system": "BigQuery", "entity": "GOLD_DB.fact_video_performa_daily"}
WHITELIST_SHEETS = {"ian"} 
# WHITELIST_SHEETS = {"ian", "imam", "riwa", "matz", "deni"} # All Passed

# BigQuery targets summarized in every alert email (reminder of what this
# project updates). Full project.dataset.table paths.
BQ_TARGETS = [
    {"table": TGT_BQ_BRONZE_VIDEO["entity"], "action": "append"},
    {"table": TGT_BQ_BRONZE_PRODUCTION["entity"], "action": "append"},
    {"table": TGT_BQ_SILVER_VIDEO["entity"], "action": "MERGE (silver upsert)"},
    {"table": TGT_BQ_SILVER_PRODUCTION["entity"], "action": "MERGE (silver upsert)"},
    {"table": TGT_GOLD["entity"], "action": "CREATE OR REPLACE (gold rebuild)"},
]

# Populated as the run advances through its write stages; read by main.py's
# exception handler so the failure email knows the stage + rollback story.
_failure_ctx = {
    "stage": "",
    "minio_files": [],
    "rollback_hint": "",
    "rollback_command": "",
    "auto_rollback_note": "",
}


def _bq_full_load(loaded_rows: dict) -> list[dict]:
    """BQ-update summary for a run that appended rows to both bronze tables,
    merged both silvers and rebuilt gold."""
    return [
        {"table": TGT_BQ_BRONZE_VIDEO["entity"], "action": "append",
         "rows": int(loaded_rows.get("video", 0))},
        {"table": TGT_BQ_BRONZE_PRODUCTION["entity"], "action": "append",
         "rows": int(loaded_rows.get("produksi", 0))},
        {"table": TGT_BQ_SILVER_VIDEO["entity"], "action": "MERGE (silver upsert)"},
        {"table": TGT_BQ_SILVER_PRODUCTION["entity"], "action": "MERGE (silver upsert)"},
        {"table": TGT_GOLD["entity"], "action": "CREATE OR REPLACE (gold rebuild)"},
    ]


def _bq_noop() -> list[dict]:
    """BQ-update summary for a successful run with no new rows to load."""
    return [
        {"table": TGT_BQ_BRONZE_VIDEO["entity"], "action": "no change", "rows": 0},
        {"table": TGT_BQ_BRONZE_PRODUCTION["entity"], "action": "no change", "rows": 0},
        {"table": TGT_BQ_SILVER_VIDEO["entity"], "action": "no change"},
        {"table": TGT_BQ_SILVER_PRODUCTION["entity"], "action": "no change"},
        {"table": TGT_GOLD["entity"], "action": "no change"},
    ]


def _merge_watermark_updates(*updates: dict) -> dict:
    """Combine per-dataset watermark-update dicts into one email view.

    Both datasets (video + produksi) render as (creds, sheet_name, grain) ->
    max_date. On the rare key collision between the two datasets, the later
    (max) date wins so neither watermark advance is hidden.
    """
    merged = {}
    for u in updates:
        for key, date_val in (u or {}).items():
            if key not in merged or str(date_val) > str(merged[key]):
                merged[key] = date_val
    return merged


def _fmt_drift_date(val) -> str:
    """Format a drift-check date value for the gate-2 email table ('' for NaT)."""
    try:
        if val is None or pd.isna(val):
            return ""
        if hasattr(val, "strftime"):
            return val.strftime("%Y-%m-%d")
        return str(val)
    except Exception:
        return str(val or "")


def _build_drift_rows(status_df: pd.DataFrame) -> list[dict]:
    """Per-sheet watermark drift summary from the pre-flight check.

    Column order matches the gate-2 email table: Sheet | Toko | Sheet max date |
    Current watermark | Status (BEHIND/ok). Sorted by sheet_name then grain.
    """
    rows = []
    for _, row in status_df.iterrows():
        rows.append({
            "sheet_name": str(row.get("sheet_name") or ""),
            "toko": str(row.get("grain") or ""),
            "gsheet_max": _fmt_drift_date(row.get("sheet_max_tanggal")),
            "watermark": _fmt_drift_date(row.get("last_processed_date")),
            "status": "BEHIND" if row.get("is_behind") else "ok",
        })
    rows.sort(key=lambda r: (r["sheet_name"], r["toko"]))
    return rows


def _finish(
    watermark_records,
    note: str,
    status: str = "",
    dry_run: bool = False,
    run_key: str = "",
    watermark_updates: dict | None = None,
    bq_updates: list[dict] | None = None,
    datasets_config: dict | None = None,
):
    """Final step on every exit path: show both datasets' watermarks, print the
    ETL DONE line, then send the success alert (skipped in dry-run).

    `watermark_records` is the {dataset_name: records} dict accumulated during
    the run; each dataset's own viewer shows its watermark before the close.
    """
    for name, cfg in (datasets_config or {}).items():
        recs = watermark_records.get(name, []) if isinstance(watermark_records, dict) else []
        show_watermark(recs)
        print(f"[WATERMARK][{name}] Lihat watermark di atas (path: {cfg['watermark_path']})")
    print(note)
    subject, body_html = build_pipeline_success_email(
        run_key=run_key,
        log_path=f"logs/run_{run_key}/etl_full_{run_key}.log" if run_key else "",
        status=status,
        watermark_updates=watermark_updates,
        bq_updates=bq_updates,
    )
    send_alert_email(subject, body_html, dry_run=dry_run)

def _get_credentials():
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path:
        raise RuntimeError("Env GOOGLE_APPLICATION_CREDENTIALS belum di-set")
    return service_account.Credentials.from_service_account_file(sa_path)


def _fetch_existing_bronze_hashes(
    creds, table_id, project_id=PROJECT_ID
) -> set:
    """Returns the set of row_hash_raw already present in Bronze.

    Used as an append-time idempotency gate: boundary-day rows (tanggal ==
    watermark) and re-loaded recovery rows are legitimately re-selected by the
    watermark filter every run; this drops the ones whose content is unchanged,
    so Bronze stops accumulating duplicates while still accepting edits (a
    changed row produces a NEW hash and flows through).
    """
    from pandas_gbq import read_gbq

    df_hashes = read_gbq(
        f"SELECT DISTINCT row_hash_raw FROM `{project_id}.{table_id}`",
        project_id=project_id,
        credentials=creds,
        dialect="standard",
    )
    return set(df_hashes["row_hash_raw"].dropna().astype(str))


def _select_recovered(
    df_valid: pd.DataFrame, resolved: list, report: dict, date_col: str = "Tanggal", grain_col: str = "toko"
) -> pd.DataFrame:
    """PATH A: select rows from df_valid that were recovered from a resolved error.

    Grain is (sheet_name, creds, grain, error_date) — grain verbatim, empty when the
    column is absent.
    A resolved entry means the key was in the error manifest last run but is NO LONGER
    in df_error this run (the data got fixed). Those rows bypass the watermark filter downstream.

    Full recovery only: we include the key's rows ONLY when the number of
    valid rows now equals the manifest n_rows. Otherwise the group is either
    only partially fixed (some rows still bad -> entry stays open) or extra
    rows appeared on that historical date. Skipping avoids duplicates and
    partial/incorrect recovery; the data is never silently lost because the
    entry remains "open" and will be retried on a later run.

    Counters are added to `report`:
      recovery_resolved        : resolved keys considered
      recovery_recovered_rows  : rows selected for Path A
      recovery_count_mismatch  : keys fixed but row_count != n_rows (skipped)
      recovery_absent          : resolved keys with no matching rows (deleted)
    """
    df = df_valid.copy()

    if df.empty or not resolved:
        report.setdefault("recovery_resolved", 0)
        report.setdefault("recovery_recovered_rows", 0)
        report.setdefault("recovery_count_mismatch", 0)
        report.setdefault("recovery_absent", 0)
        return df.iloc[0:0]

    raw_grain = next(
        (c for c in df.columns if str(c).strip().lower() == grain_col.lower()), None
    )
    if raw_grain is not None:
        toko_series = df[raw_grain].astype(str)
    else:
        toko_series = pd.Series("", index=df.index, dtype=str)

    try:
        tanggal_str = pd.to_datetime(df[date_col]).dt.date.astype(str)
    except Exception:
        tanggal_str = df[date_col].astype(str)

    key_series = (
        df["sheet_name"].astype(str)
        + "|" + df["creds"].astype(str)
        + "|" + toko_series
        + "|" + tanggal_str
    )

    match = pd.Series(False, index=df.index)
    count_mismatch = 0
    absent = 0

    for r in resolved:
        key = f'{r["sheet_name"]}|{r["creds"]}|{r.get("toko") or ""}|{r["error_date"]}'
        grp = df.index[key_series == key]
        n_expected = int(r.get("n_rows") or 0)

        if len(grp) == 0:
            absent += 1                      # rows removed from sheet
        elif len(grp) == n_expected:
            match.loc[grp] = True            # fully recovered -> Path A
        else:
            count_mismatch += 1              # FIXED but count mismatch -> skip

    report["recovery_resolved"] = len(resolved)
    report["recovery_recovered_rows"] = int(match.sum())
    report["recovery_count_mismatch"] = count_mismatch
    report["recovery_absent"] = absent

    return df[match]

def _write_wm_log(log_folder, run_key, video_status, produksi_status, video_sheet_passes, verdict_msg):
    """Write the watermark drift check log file."""
    wm_log_lines = []
    wm_log_lines.append(f"=== WATERMARK DRIFT CHECK — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")

    wm_log_lines.append("-" * 50)
    wm_log_lines.append("DATASET: PERFORMA VIDEO (toko grain)")
    wm_log_lines.append("-" * 50)
    wm_log_lines.append(f"  {'sheet':<10} {'grain':<12} {'gsheet':<12} {'wm':<12} {'status'}")
    for _, row in video_status.iterrows():
        wm_log_lines.append(
            f"  {row['sheet_name']:<10} {str(row['grain']):<12} "
            f"{str(row['sheet_max_tanggal']):<12} {str(row['last_processed_date']):<12} "
            f"{'BEHIND' if row['is_behind'] else 'ok'}"
        )

    wm_log_lines.append("")
    wm_log_lines.append("-" * 50)
    wm_log_lines.append("DATASET: PRODUKSI (akun grain)")
    wm_log_lines.append("-" * 50)
    wm_log_lines.append(f"  {'sheet':<10} {'grain':<12} {'gsheet':<12} {'wm':<12} {'status'}")
    for _, row in produksi_status.iterrows():
        wm_log_lines.append(
            f"  {row['sheet_name']:<10} {str(row['grain']):<12} "
            f"{str(row['sheet_max_tanggal']):<12} {str(row['last_processed_date']):<12} "
            f"{'BEHIND' if row['is_behind'] else 'ok'}"
        )

    wm_log_lines.append(f"\nGate verdict: {verdict_msg}")
    video_pass_count = int(video_sheet_passes.sum())
    video_total = len(video_sheet_passes)
    produksi_behind = int(produksi_status["is_behind"].sum())
    produksi_total = len(produksi_status)
    wm_log_lines.append(f"  video: {video_pass_count}/{video_total} sheets have >=1 toko behind")
    wm_log_lines.append(f"  produksi: {produksi_behind}/{produksi_total} akun behind")

    write_section_log(log_folder, f"wm_monitor_logs_{run_key}.log", "\n".join(wm_log_lines) + "\n")


def _write_failure_log(log_folder, run_key, message: str):
    """Write a gate-abort failure log file for this run."""
    f_path = write_section_log(
        log_folder,
        f"etl_failed_{run_key}.log",
        f"ETL FAILED because: {message}\n",
    )
    print(f"[FATAL] ETL failed because: {message}")
    print(f"[FATAL] Failure written to: {f_path}")


def run_daily_etl(dry_run: bool | None = None):
    print("== Start ETL Performa Video ==")

    if dry_run is None:
        dry_run = os.getenv("ETL_DRY_RUN", "0").strip().lower() in {"1", "true", "yes", "y"}

    if dry_run:
        print("[DRY-RUN] Mode aktif: TIDAK ada data yang ditulis ke MinIO/BigQuery/Silver.")

    # Failure context tracker for main.py's exception handler. Cleared on each
    # run so a stale stage never leaks into a later run's failure email.
    _failure_ctx.clear()
    _failure_ctx.update({
        "stage": "", "minio_files": [], "rollback_hint": "",
        "rollback_command": "", "auto_rollback_note": "",
    })

    # 1) Client
    gc = get_gspread_client()
    creds = _get_credentials()
    minio_client, minio_bucket = get_minio_client()

    # 2) Date keys & Cutoff
    #    partition pakai YYYYMMDD, nama file pakai YYYYMMDDHHMM
    #    (jam+menit agar 2 run di hari yang sama menghasilkan file terpisah, tanpa overwrite).
    now_obj = datetime.now()
    today_key = now_obj.strftime("%Y%m%d")
    run_key = now_obj.strftime("%Y%m%d%H%M")
    log_folder = get_log_folder(run_key) if not dry_run else None

    # Structured event logging (JSON-lines). In standalone runs, set up a fresh
    # event log so the pipeline events are still emitted.
    if not is_event_logging_enabled():
        _, stop_event_logging = setup_event_logging(run_key, dry_run)
    else:
        stop_event_logging = None
    emit("PIPELINE", "run_daily_etl", "ETL start",
         metrics={"dry_run": dry_run},
         source={"system": "Google_Sheets", "entity": "video, produksi"},
         target=TGT_BQ_SILVER_VIDEO)

    # 2B) PRE-FLIGHT: Watermark drift gate
    spreadsheet_objects = init_spreadsheet_objects(gc)

    print("\n" + "=" * 70)
    print("--- PRE-FLIGHT: Watermark Drift Check ---")
    print("=" * 70)
    video_status = performa_video_watermark_check(spreadsheet_objects, minio_client, minio_bucket)
    produksi_status = produksi_watermark_check(spreadsheet_objects, minio_client, minio_bucket)

    video_view = pd.DataFrame({
        "sheet_name": video_status["sheet_name"],
        "toko": video_status["grain"],
        "gsheet": video_status["sheet_max_tanggal"],
        "wm": video_status["last_processed_date"],
        "flag": video_status["is_behind"].map({True: "BEHIND", False: "ok"}),
    })
    print("-" * 70)
    print("DATASET: PERFORMA VIDEO (toko grain)")
    print("-" * 70)
    print(video_view.sort_values(["sheet_name", "toko"]).to_string(
        index=False,
        formatters={
            "gsheet": lambda v: v.strftime("%Y-%m-%d") if pd.notna(v) else "",
            "wm": lambda v: v.strftime("%Y-%m-%d") if pd.notna(v) else "",
        },
    ))
    print()

    produksi_view = pd.DataFrame({
        "sheet_name": produksi_status["sheet_name"],
        "akun": produksi_status["grain"],
        "gsheet": produksi_status["sheet_max_tanggal"],
        "wm": produksi_status["last_processed_date"],
        "flag": produksi_status["is_behind"].map({True: "BEHIND", False: "ok"}),
    })
    print("-" * 70)
    print("DATASET: PRODUKSI (akun grain)")
    print("-" * 70)
    print(produksi_view.sort_values(["sheet_name", "akun"]).to_string(
        index=False,
        formatters={
            "gsheet": lambda v: v.strftime("%Y-%m-%d") if pd.notna(v) else "",
            "wm": lambda v: v.strftime("%Y-%m-%d") if pd.notna(v) else "",
        },
    ))
    print()

    n_video_behind = int(video_status["is_behind"].sum()) if len(video_status) else 0
    n_produksi_behind = int(produksi_status["is_behind"].sum()) if len(produksi_status) else 0
    n_video_sheets = video_status.groupby("sheet_name")["is_behind"].any()
    emit("PRE_FLIGHT", "watermark_monitor", "Watermark drift check complete",
         metrics={
             "video_rows": int(len(video_status)),
             "video_groups_behind": n_video_behind,
             "video_sheets_behind": int(n_video_sheets.sum()) if len(n_video_sheets) else 0,
             "produksi_rows": int(len(produksi_status)),
             "produksi_behind": n_produksi_behind,
         },
         source=SRC_MINIO)

    # Gate 1: abort on access errors (enforced in dry-run too).
    # Empty status (no watermark yet / first run) -> no errors, let gates 2-3 pass.
    video_sheet_passes = video_status.groupby("sheet_name")["is_behind"].any()
    all_status = pd.concat([video_status, produksi_status], ignore_index=True)
    has_errors = bool(len(all_status)) and all_status["status"].str.startswith("error").any()
    if has_errors:
        error_sheets = all_status[all_status["status"].str.startswith("error")]["sheet_name"].unique().tolist()
        mode = "DRY-RUN WOULD ABORT" if dry_run else "ABORT"
        print(f"[GATE] {mode} — access errors on sheets: {error_sheets}")
        print("[GATE] Fix the sheet access issue and re-run.")
        emit("GATE", "watermark_monitor",
             f"{mode} — access errors on sheets: {error_sheets}",
             level="ERROR",
             metrics={"error_sheets": error_sheets},
             source=SRC_MINIO)
        subject, body_html = build_gate_abort_email(
            gate="1",
            mode=mode,
            message=f"Access errors while checking sheets: {error_sheets}",
            lists={"error_sheets": error_sheets},
            note="Fix the sheet access issue and re-run.",
            bq_updates=BQ_TARGETS,
        )
        send_alert_email(subject, body_html, dry_run=dry_run)
        if log_folder:
            _write_wm_log(log_folder, run_key, video_status, produksi_status, video_sheet_passes,
                          f"ABORT — access errors on sheets: {error_sheets}")
            _write_failure_log(
                log_folder, run_key,
                f"access errors on sheets: {error_sheets}",
            )
        return

    # Gate 2: each video sheet must have >=1 toko behind (enforced in dry-run too)
    caught_up = [s for s in video_sheet_passes[~video_sheet_passes].index.tolist() if s not in WHITELIST_SHEETS]
    behind_sheets_list = video_sheet_passes[video_sheet_passes].index.tolist()
    whitelisted_skipped = [s for s in video_sheet_passes[~video_sheet_passes].index.tolist() if s in WHITELIST_SHEETS]

    if caught_up:
        mode = "DRY-RUN WOULD ABORT" if dry_run else "ABORT"
        print(f"[GATE] Video sheets already up-to-date (skipped): {caught_up}")
        print(f"[GATE] Video sheets with new data: {behind_sheets_list}")
        if whitelisted_skipped:
            print(f"[GATE] Whitelisted video sheets up-to-date (not blocking): {whitelisted_skipped}")
        print(f"[GATE] {mode} — video sheets with no new data are required before continuing.")

        toko_detail = []
        for sheet_name in caught_up:
            sel = video_status[video_status["sheet_name"] == sheet_name]
            toko_without_new = sel[~sel["is_behind"]]["grain"].tolist()
            toko_detail.append({"sheet_name": sheet_name,
                                "toko_without_new_data": toko_without_new})

        emit(
            "GATE", "watermark_monitor",
            f"{mode} — video sheets with no new data: {caught_up}",
            level="ERROR",
            metrics={
                "caught_up": caught_up,
                "behind": behind_sheets_list,
                "whitelisted_skipped": whitelisted_skipped,
                "caught_up_detail": toko_detail,
                "total_groups": int(len(video_status)),
                "sheets_up_to_date": len(caught_up),
                "sheets_with_new_data": len(behind_sheets_list),
            },
            source=SRC_MINIO,
        )
        subject, body_html = build_gate_abort_email(
            gate="2",
            mode=mode,
            message=(
                "Video sheets with no new data are required before continuing: "
                f"{caught_up}"
            ),
            lists={
                "sheets_up_to_date": caught_up,
                "behind_sheets": behind_sheets_list,
                "whitelisted_skipped": whitelisted_skipped,
            },
            note=(
                "Some video sheets are already up-to-date (watermark >= sheet max). "
                "Produksi is never gated — only toko-grain video sheets require "
                ">=1 toko behind. If this is expected (no genuinely new data), no "
                "action needed; otherwise check the source sheet dates."
            ),
            bq_updates=BQ_TARGETS,
            drift_rows=_build_drift_rows(video_status),
        )
        send_alert_email(subject, body_html, dry_run=dry_run)
        if log_folder:
            _write_wm_log(log_folder, run_key, video_status, produksi_status, video_sheet_passes,
                          f"ABORT — video sheets with no new data: {caught_up}")
            _write_failure_log(
                log_folder, run_key,
                f"video sheets with no new data: {caught_up}",
            )
        return

    # NOTE: produksi is NOT gated on having new data. Only the toko-grain
    # video sheets (Gate 2) require >=1 toko behind. Produksi always runs, but
    # remains informational in the drift report / wm_monitor log.
    print("--- PRE-FLIGHT PASSED — video sheets have new data ---\n")

    # Write wm_monitor log (always, even in dry-run)
    if log_folder:
        video_pass_count = int(video_sheet_passes.sum())
        video_total = len(video_sheet_passes)
        produksi_behind = int(produksi_status["is_behind"].sum())
        produksi_total = len(produksi_status)
        verdict = (
            f"PASS — video: {video_pass_count}/{video_total} sheets, "
            f"produksi: {produksi_behind}/{produksi_total} akun"
        )
        _write_wm_log(log_folder, run_key, video_status, produksi_status, video_sheet_passes, verdict)

    # 3) Ingest dari GSheet (tiap sheet di-tag sheet_name)
    df_tt_vid_raw = fetch_tiktok_video(gc, spreadsheet_objects)
    df_tt_prod_raw = fetch_tiktok_produksi(gc, spreadsheet_objects)
    print(f"[INGEST] Rows raw video from GSheet: {len(df_tt_vid_raw)}")
    print(f"[INGEST] Rows raw produksi from GSheet: {len(df_tt_prod_raw)}")
    emit("INGEST", "gsheet_ingester", "Fetched raw rows from GSheets",
         metrics={"video_rows_raw": int(len(df_tt_vid_raw)),
                  "produksi_rows_raw": int(len(df_tt_prod_raw))},
         source={"system": "Google_Sheets", "entity": "video, produksi"})

    # 4) Definisikan konfigurasi tiap dataset (build fn, raw df, path file & path watermark)
    datasets_config = {
        "video": {
            "build_fn": build_bronze_video,
            "raw_df": df_tt_vid_raw,
            "id_col": "tanggal",
            "date_cols": ["tanggal", "waktu"],
            "numeric_cols": NUMERIC_COLS,
            "percent_cols": PERCENT_COLS,
            "file_path": f"performa/video/date={today_key}/video_{run_key}.parquet",
            "watermark_path": "watermarks/performa_video.json",
            "manifest_path": "error_list_watermark/video/error_manifest.json",
            "fix_prefix": "fix_error_list_watermark/video",
            "bq_table_id": "BRONZE_DB.bronze_video",
            "grain_col": "toko",
            "grain_field": "toko",
        },
        "produksi": {
            "build_fn": build_bronze_produksi,
            "raw_df": df_tt_prod_raw,
            "id_col": "id_konten",
            "date_cols": ["tanggal", "tanggal_jadi"],
            "numeric_cols": [],
            "percent_cols": None,
            "file_path": f"produksi/date={today_key}/produksi_{run_key}.parquet",
            "watermark_path": "watermarks/produksi.json",
            "manifest_path": "error_list_watermark/produksi/error_manifest.json",
            "fix_prefix": "fix_error_list_watermark/produksi",
            "bq_table_id": "BRONZE_DB.bronze_video_production",
            "grain_col": "akun",
            "grain_field": "akun",
        },
    }

    # 5) Loop pemrosesan per-sheet incremental & upload terpisah untuk setiap dataset
    #    Resolve SHEET_REGISTRY (name -> env key) ke nilai creds (spreadsheet ID) sekali,
    #    supaya FAILSAFE migrasi format lama (sheet_name -> creds) memetakan ke ID yang benar.
    sheet_registry = {name: os.getenv(env_key) for name, env_key in SHEET_REGISTRY.items()}
    sheet_registry["produksi"] = os.getenv("SH_KEY_PRODUKSI")
    dataset_watermarks = {}
    loaded_rows = {}
    dataset_watermark_updates = {}
    for name, cfg in datasets_config.items():
        print(f"\n--- Processing dataset: {name.upper()} ---")
        watermark_path = cfg["watermark_path"]
        file_path = cfg["file_path"]

        src = {"system": "Google_Sheets", "entity": name}
        src_wm = {"system": "MinIO", "entity": cfg["watermark_path"]}
        tgt_minio = {"system": "MinIO", "entity": cfg["file_path"].rsplit("/", 1)[0]}
        tgt_bq = {"system": "BigQuery", "entity": f"{PROJECT_ID}.{cfg['bq_table_id']}"}
        tgt_q = {"system": "MinIO", "entity": f"quarantine/{name}"}

        # STEP 2: validate & normalize as early as possible (mixed-column
        #     detection + date-error capture). Runs exactly once, before anything else.

        # buang baris tanpa id
        key_col = next(
            c for c in cfg["raw_df"].columns if to_snake_case(str(c)) == cfg["id_col"]
        )
        df_raw = cfg["raw_df"]
        df_raw = df_raw[df_raw[key_col].astype(str).str.strip() != ""]

        date_cols = [
            next(
                c for c in df_raw.columns if to_snake_case(str(c)) == dc
            )
            for dc in cfg["date_cols"]
        ]
        date_col = date_cols[0] if date_cols else None
        df_valid, df_error, v_report = validate_and_normalize_raw(
            df_raw, cfg["numeric_cols"], percent_cols=cfg["percent_cols"], date_cols=date_cols
        )
        print(
            f"[VALIDATE] Rows valid: {len(df_valid)} | bad rows: {v_report['n_bad_rows']} "
            f"(date errors: {v_report['n_date_errors']} | future date errors: {v_report.get('n_date_future',0)}) | blank rows dropped: {v_report['n_blank_rows']}"
        )
        emit(
            "VALIDATE", "validator",
            f"[{name}] Validated: {len(df_valid)} valid, {v_report['n_bad_rows']} bad",
            level="WARN" if v_report["n_bad_rows"] else "INFO",
            metrics={
                "dataset": name,
                "rows_raw": int(len(df_raw)),
                "rows_valid": int(len(df_valid)),
                "n_bad_rows": int(v_report["n_bad_rows"]),
                "n_date_errors": int(v_report["n_date_errors"]),
                "n_date_future": int(v_report.get("n_date_future", 0)),
                "n_blank_rows": int(v_report["n_blank_rows"]),
            },
            source=src,
            target=tgt_q,
        )
        if v_report["has_changes"]:
            print(f"[VALIDATE] Corrupted/Shifted columns: {v_report['affected_columns']}")
            print(
                f"[VALIDATE] Affected date range: {v_report['first_affected_date']} "
                f"---> {v_report['last_affected_date']}"
            )

        # STEP 3Q/6: sync error manifest EVERY run (append new open entries +
        # resolve entries whose format has been fixed since the last run).
        # Resolved entries feed PATH A (error recovery) below.
        resolved = sync_error_manifest(minio_client, minio_bucket, df_error, v_report, today_key, run_key, subfolder=name, manifest_path=cfg["manifest_path"], fix_prefix=cfg["fix_prefix"], date_col=date_col, df_valid=df_valid, dry_run=dry_run, grain_col=cfg["grain_col"])

        if not df_error.empty:
            # Shared by the quarantine log and the notify alert.
            if "error_reason" in df_error.columns:
                all_reasons = df_error["error_reason"].str.split("|").explode()
                reason_counts = all_reasons.value_counts().to_dict()
                col_pattern = df_error["error_reason"].str.findall(r"date_unparsable\((\w+)=")
                affected_from_dates = set()
                for cols in col_pattern:
                    affected_from_dates.update(cols)
                affected_cols = sorted(affected_from_dates | set(v_report.get("affected_columns", [])))
            else:
                reason_counts = None
                affected_cols = []

            if dry_run:
                print(f"[DRY-RUN][{name}] Akan quarantine {len(df_error)} bad row(s)")
            else:
                write_quarantine(minio_client, minio_bucket, df_error, today_key, run_key, subfolder=name)

                # Write quarantine log with summary + sample bad rows
                if log_folder:
                    import re as _re
                    q_lines = []
                    q_lines.append(f"=== QUARANTINE REPORT — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
                    q_lines.append(f"Dataset: {name}")
                    q_lines.append(f"  Total quarantined rows: {len(df_error)}\n")

                    if reason_counts is not None:
                        q_lines.append("  Error reasons breakdown:")
                        for reason, count in reason_counts.items():
                            q_lines.append(f"    {reason} : {count} rows")
                        q_lines.append("")

                        if affected_cols:
                            q_lines.append(f"  Affected columns: {affected_cols}\n")

                    sample = df_error.head(5)
                    q_lines.append(f"  Sample bad rows (first {len(sample)}):")
                    display_cols = [c for c in ["Tanggal", "tanggal", "Toko", "toko", "Akun", "akun",
                                                 "VV", "Likes", "error_reason"] if c in sample.columns]
                    if display_cols:
                        header = " | ".join(f"{c:<15}" for c in display_cols)
                        q_lines.append(f"    | {header} |")
                        q_lines.append(f"    | {'-' * len(header)} |")
                        for _, r in sample.iterrows():
                            vals = " | ".join(f"{str(r.get(c, '')):<15}" for c in display_cols)
                            q_lines.append(f"    | {vals} |")

                    q_lines.append("")
                    write_section_log(log_folder, f"quarantine_errors_{run_key}.log", "\n".join(q_lines) + "\n")
            emit(
                "QUARANTINE", "validator",
                f"[{name}] {len(df_error)} bad row(s) quarantined",
                level="WARN",
                metrics={"dataset": name, "n_quarantined": int(len(df_error))},
                source=src,
                target=tgt_q,
            )
            subject, body_html = build_quarantine_email(
                len(df_error),
                reason_counts,
                affected_cols,
                sample_rows=[
                    {c: r[c] for c in QUARANTINE_SAMPLE_COLUMNS if c in df_error.columns}
                    for _, r in df_error.head(QUARANTINE_SAMPLE_ROWS).iterrows()
                ],
                minio_path=(
                    f"{QUARANTINE_PREFIX}/{name}/date={today_key}/quarantine_{run_key}.parquet"
                    if not dry_run
                    else ""
                ),
                log_path=(
                    os.path.join(log_folder, f"quarantine_errors_{run_key}.log")
                    if log_folder
                    else ""
                ),
                bq_updates=BQ_TARGETS,
            )
            send_alert_email(subject, body_html, dry_run=dry_run)

        # Get per-sheet watermark spesifik untuk dataset ini
        watermark_map, watermark_records = get_sheet_watermarks(
            minio_client, minio_bucket, watermark_path, grain_field=cfg["grain_field"]
        )
        dataset_watermarks[name] = watermark_records

        # PATH A: recovered rows (fixed since last run) bypass the watermark.
        df_recovered = _select_recovered(df_valid, resolved, v_report, date_col, grain_col=cfg["grain_col"])
        print(
            f"[RECOVERY][{name}] resolved={v_report.get('recovery_resolved', 0)} "
            f"| recovered_rows={v_report.get('recovery_recovered_rows', 0)} "
            f"| absent={v_report.get('recovery_absent', 0)} "
            f"| count_mismatch_skipped={v_report.get('recovery_count_mismatch', 0)}"
        )
        emit(
            "RECOVERY", "error_recovery",
            f"[{name}] Recovery: resolved={v_report.get('recovery_resolved', 0)}, "
            f"recovered_rows={v_report.get('recovery_recovered_rows', 0)}, "
            f"absent={v_report.get('recovery_absent', 0)}, "
            f"count_mismatch_skipped={v_report.get('recovery_count_mismatch', 0)}",
            level="WARN" if v_report.get("recovery_count_mismatch", 0) or v_report.get("recovery_absent", 0) else "INFO",
            metrics={
                "dataset": name,
                "resolved": int(v_report.get("recovery_resolved", 0)),
                "recovered_rows": int(v_report.get("recovery_recovered_rows", 0)),
                "absent": int(v_report.get("recovery_absent", 0)),
                "count_mismatch_skipped": int(v_report.get("recovery_count_mismatch", 0)),
            },
            source=src,
            target=tgt_bq,
        )
        if v_report.get("recovery_recovered_rows", 0) or v_report.get("recovery_resolved", 0):
            subject, body_html = build_recovery_email(
                resolved=v_report.get("recovery_resolved", 0),
                recovered_rows=v_report.get("recovery_recovered_rows", 0),
                absent=v_report.get("recovery_absent", 0),
                count_mismatch_skipped=v_report.get("recovery_count_mismatch", 0),
                dataset_name=name,
                bq_updates=BQ_TARGETS,
            )
            send_alert_email(subject, body_html, dry_run=dry_run)

        # PATH B: remaining rows use the standard per-sheet watermark filter.
        df_regular = df_valid.drop(df_recovered.index)
        df_filtered, sheet_max_dates = cfg["build_fn"](
            df_regular, sheet_watermarks=watermark_map
        )

        # PATH A transform: empty watermarks = full load, max dates fed into the
        # per-sheet watermark so recovered rows are not re-selected every run.
        if df_recovered.empty:
            df_recovered_bronze = df_filtered.iloc[0:0]
        else:
            df_recovered_bronze, recovered_max_dates = cfg["build_fn"](
                df_recovered, sheet_watermarks={}
            )
            for key, max_date in recovered_max_dates.items():
                sheet_max_dates[key] = max(sheet_max_dates.get(key, max_date), max_date)

        # Accumulate this dataset's candidate watermark advances so the combined
        # success email can render both datasets' updates (guarded against key
        # collisions via _merge_watermark_updates).
        dataset_watermark_updates[name] = sheet_max_dates.copy()

        # MERGE & DEDUPLICATE
        df_filtered = pd.concat(
            [df_filtered, df_recovered_bronze], ignore_index=True
        ).drop_duplicates(subset=["row_hash_raw"])

        # Idempotency gate: drop rows whose content hash already exists in Bronze.
        # Boundary-day re-emissions and previously-recovered rows are re-selected by
        # the watermark each run; only genuinely new/changed rows should be appended.
        if not df_filtered.empty:
            existing_hashes = _fetch_existing_bronze_hashes(creds, table_id=cfg["bq_table_id"])
            if existing_hashes:
                before = len(df_filtered)
                df_filtered = df_filtered[
                    ~df_filtered["row_hash_raw"].astype(str).isin(existing_hashes)
                ]
                skipped = before - len(df_filtered)
                if skipped:
                    print(
                        f"[IDEMPOTENCY][{name}] Skipped {skipped} row(s) already present in bronze"
                    )
                    emit(
                        "BRONZE", "idempotency_gate",
                        f"[{name}] Skipped {skipped} row(s) already present in bronze",
                        level="INFO",
                        metrics={"dataset": name, "skipped": int(skipped)},
                        source=src,
                        target=tgt_bq,
                    )

        print(f"[BRONZE] Rows bronze {name} to load: {len(df_filtered)}")
        emit("BRONZE", "bronze_builder", f"Rows bronze {name} to load: {len(df_filtered)}",
             metrics={"dataset": name, "rows_loaded": int(len(df_filtered))},
             source=src, target=tgt_bq)

        # Nothing new to append: if rows were still selected (boundary-day /
        # recovered re-emissions) but every one already exists in Bronze, advance
        # the watermark anyway so they stop being re-selected every run.
        if df_filtered.empty and sheet_max_dates:
            if dry_run:
                changes = effective_watermark_changes(watermark_records, sheet_max_dates)
                for (sheet_key, sheet_name, toko), max_date in changes.items():
                    print(f"[DRY-RUN][{name}]   watermark update ({sheet_key}, {sheet_name}, {toko}) -> {max_date}")
                if changes:
                    print(f"[DRY-RUN][{name}] Akan update watermark {name} (tanpa upload parquet).")
                else:
                    print(f"[DRY-RUN][{name}] Tidak ada perubahan watermark {name} (nilai sudah sama).")
                emit("FINISH", "etl_pipeline",
                     f"[{name}] ETL DONE (DRY-RUN) - watermark only",
                     metrics={"dataset": name, "watermark_changes": len(changes)},
                     source=src, target=src_wm)
                continue
            update_sheet_watermarks(
                minio_client, minio_bucket, watermark_path, watermark_records,
                sheet_max_dates, grain_field=cfg["grain_field"],
            )
            print(f"[MINIO][{name}] Watermark advanced (no new rows to append).")
            continue

        if df_filtered.empty:
            print(f"[MINIO] Skip uploading {name}, no new rows available to process.")
            emit("FINISH", "etl_pipeline",
                 f"[{name}] ETL DONE - no new data to process",
                 metrics={"dataset": name, "rows_loaded": 0},
                 source=src, target=src_wm)
            continue

        # Folder partition marker
        folder_path = f"{file_path.rsplit('/', 1)[0]}/"

        if dry_run:
            print(f"[DRY-RUN][{name}] Akan upload {len(df_filtered)} baris ke '{file_path}'")
            changes = effective_watermark_changes(watermark_records, sheet_max_dates)
            for (sheet_key, sheet_name, toko), max_date in changes.items():
                print(f"[DRY-RUN][{name}]   watermark update ({sheet_key}, {sheet_name}, {toko}) -> {max_date}")
            if not changes:
                print(f"[DRY-RUN][{name}] Tidak ada perubahan watermark (nilai sudah sama).")
            print(f"[DRY-RUN][{name}] Akan: append ke {cfg['bq_table_id']}")
            emit("FINISH", "etl_pipeline",
                 f"[{name}] ETL DONE (DRY-RUN) - would load {len(df_filtered)} rows",
                 metrics={"dataset": name, "rows_loaded": int(len(df_filtered)),
                          "watermark_changes": len(changes)},
                 source=src, target=tgt_bq)
            continue

        minio_client.put_object(minio_bucket, folder_path, io.BytesIO(b""), length=0)

        # Upload parquet ke MinIO
        try:
            parquet_bytes = df_filtered.to_parquet(index=False, engine="pyarrow")
            minio_client.put_object(
                minio_bucket,
                file_path,
                io.BytesIO(parquet_bytes),
                length=len(parquet_bytes),
                content_type="application/octet-stream",
            )
        except Exception as e:
            emit(
                "LOAD", "minio_loader",
                f"[{name}] Parquet upload failed: {type(e).__name__} {e}",
                level="ERROR",
                metrics={"dataset": name, "rows": int(len(df_filtered)), "target_path": file_path},
                source=src,
                target=tgt_minio,
            )
            if log_folder:
                err_lines = [
                    f"=== BRONZE/PARQUET ERROR — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===",
                    f"\nDataset: {name}",
                    f"  Stage: Parquet upload",
                    f"  Target path: {file_path}",
                    f"  Rows: {len(df_filtered)}",
                    f"  Error: {type(e).__name__} {e}\n",
                    f"  Traceback:\n{traceback.format_exc()}",
                ]
                write_section_log(log_folder, f"bronze_parquet_errors_{run_key}.log", "\n".join(err_lines) + "\n")
            _failure_ctx.update({
                "stage": f"MinIO parquet upload — {name}",
                "minio_files": [folder_path, file_path],
                "rollback_hint": (
                    "Watermark is NOT yet updated and BigQuery is untouched. "
                    "A partial parquet may exist in MinIO; a re-run is safe "
                    "and needs no rollback."
                ),
            })
            raise
        print(f"[MINIO] Successfully Loaded {name} to: {file_path}")
        emit("LOAD", "minio_loader", f"[{name}] Parquet uploaded to {file_path}",
             metrics={"dataset": name, "rows": int(len(df_filtered)), "target_path": file_path},
             source=src, target=tgt_minio)

        # Load bronze ke BigQuery (per dataset)
        try:
            load_df(
                df_filtered,
                table_id=cfg["bq_table_id"],
                project_id=PROJECT_ID,
                if_exists="append",
                credentials=creds,
            )
        except Exception as e:
            emit(
                "LOAD", "bigquery_loader",
                f"[{name}] BigQuery load failed: {type(e).__name__} {e}",
                level="ERROR",
                metrics={"dataset": name, "rows_loaded": int(len(df_filtered)),
                         "table": f"{PROJECT_ID}.{cfg['bq_table_id']}", "if_exists": "append"},
                source=src_wm,
                target=tgt_bq,
            )
            if log_folder:
                err_lines = [
                    f"=== BRONZE/PARQUET ERROR — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===",
                    f"\nDataset: {name}",
                    f"  Stage: BigQuery load",
                    f"  Target table: {cfg['bq_table_id']}",
                    f"  Rows being loaded: {len(df_filtered)}",
                    f"  Error: {type(e).__name__} {e}\n",
                    f"  Traceback:\n{traceback.format_exc()}",
                ]
                write_section_log(log_folder, f"bronze_parquet_errors_{run_key}.log", "\n".join(err_lines) + "\n")
            # AUTO ROLLBACK: the watermark already advanced past rows that never
            # hit bronze. Restore it (watermark scope only) so a re-run re-selects
            # them. With WATERMARK_PREFIX="watermarks/", the prefix-scan restores
            # BOTH performa_video.json and produksi.json.
            rollback_note = ""
            toggle = os.getenv("ETL_AUTO_ROLLBACK_WATERMARK", "true").strip().lower()
            if toggle in {"1", "true", "yes", "y"}:
                try:
                    from src.performa_video.utils.minio_rollback import auto_restore_watermark

                    rollback_note = auto_restore_watermark(minio_client, minio_bucket, run_key)
                    print(f"[ROLLBACK] {rollback_note}")
                    emit("ROLLBACK", "minio_rollback", rollback_note,
                         level="WARN" if "DONE" in rollback_note else "INFO",
                         metrics={"run_key": run_key, "dataset": name},
                         source=src_wm, target=src_wm)
                except Exception as rb_exc:
                    rollback_note = f"AUTO ROLLBACK ERROR: {type(rb_exc).__name__}: {rb_exc}"
                    print(f"[ROLLBACK] {rollback_note}")
                    emit("ROLLBACK", "minio_rollback", rollback_note,
                         level="ERROR", metrics={"run_key": run_key, "dataset": name},
                         source=src_wm, target=src_wm)
            _failure_ctx.update({
                "stage": f"BigQuery bronze load (append) — {name}",
                "minio_files": [file_path, cfg["watermark_path"]],
                "rollback_hint": (
                    "MinIO parquet written AND the watermark already advanced, but the "
                    f"bronze append for '{name}' FAILED. Rows past the watermark are not "
                    "re-read by a plain re-run - roll the MinIO watermark(s) back before "
                    "re-running, or those rows will be skipped in BigQuery."
                ),
                "rollback_command": (
                    "python -m src.performa_video.utils.minio_rollback "
                    f"--run-key {run_key} --scope full --dry-run"
                ),
                "auto_rollback_note": rollback_note,
            })
            raise
        print(f"[BRONZE] Load to {cfg['bq_table_id']} DONE")
        loaded_rows[name] = int(len(df_filtered))
        emit("LOAD", "bigquery_loader", f"[{name}] Bronze load to {cfg['bq_table_id']} DONE",
             metrics={"dataset": name, "rows_loaded": int(len(df_filtered)),
                      "table": f"{PROJECT_ID}.{cfg['bq_table_id']}"},
             source=src_wm, target=tgt_bq)

        # Update per-sheet watermark masing-masing dataset
        update_sheet_watermarks(
            minio_client, minio_bucket, watermark_path, watermark_records, sheet_max_dates,
            grain_field=cfg["grain_field"],
        )

    # 4) Silver: MERGE (skipped in dry-run)
    if dry_run:
        print("[DRY-RUN] Akan MERGE ke SILVER_DB.silver_tt_video + SILVER_DB.silver_tt_video_production + build GOLD")
        emit("FINISH", "etl_pipeline", "ETL Performa Video DONE (DRY-RUN)",
             metrics={"rows_loaded": loaded_rows},
             source={"system": "Google_Sheets", "entity": "video, produksi"},
             target=TGT_GOLD)
        _finish(
            dataset_watermarks,
            "== ETL Performa Video DONE (DRY-RUN) ==",
            status="dry-run — full load (would run)",
            dry_run=dry_run,
            run_key=run_key,
            watermark_updates=_merge_watermark_updates(*dataset_watermark_updates.values()),
            bq_updates=_bq_full_load(loaded_rows),
            datasets_config=datasets_config,
        )
        return

    print("[SILVER] Running MERGE into SILVER_DB.silver_tt_video ...")
    try:
        merge_to_silver_video()
    except Exception as e:
        emit(
            "SILVER", "silver_merger",
            f"Silver MERGE failed: {type(e).__name__} {e}",
            level="ERROR",
            metrics={"table": "SILVER_DB.silver_tt_video"},
            source={"system": "BigQuery", "entity": "BRONZE_DB.bronze_video"},
            target=TGT_BQ_SILVER_VIDEO,
        )
        if log_folder:
            err_lines = [
                f"=== SILVER/GOLD ERROR — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===",
                f"\nStage: Silver MERGE",
                f"  Table: SILVER_DB.silver_tt_video",
                f"  SQL file: sql/silver_merge_tt_video.sql",
                f"  Error: {type(e).__name__} {e}\n",
                f"  Traceback:\n{traceback.format_exc()}",
            ]
            write_section_log(log_folder, f"silver_gold_errors_{run_key}.log", "\n".join(err_lines) + "\n")
        _failure_ctx.update({
            "stage": "BigQuery silver MERGE (video)",
            "minio_files": [datasets_config["video"]["file_path"], datasets_config["video"]["watermark_path"]],
            "rollback_hint": (
                "MinIO archive + bronze append succeeded; only the silver(video) MERGE failed. "
                "The silver upsert is idempotent over bronze, so a plain re-run is safe "
                "without any MinIO rollback."
            ),
        })
        raise
    print("[SILVER] MERGE VIDEO DONE")
    emit("SILVER", "silver_merger", "Silver MERGE into SILVER_DB.silver_tt_video DONE",
         metrics={"table": "SILVER_DB.silver_tt_video"},
         source={"system": "BigQuery", "entity": "BRONZE_DB.bronze_video"},
         target=TGT_BQ_SILVER_VIDEO)

    try:
        merge_to_silver_production()
    except Exception as e:
        emit(
            "SILVER", "silver_merger",
            f"Silver MERGE failed: {type(e).__name__} {e}",
            level="ERROR",
            metrics={"table": "SILVER_DB.silver_tt_video_production"},
            source={"system": "BigQuery", "entity": "BRONZE_DB.bronze_video_production"},
            target=TGT_BQ_SILVER_PRODUCTION,
        )
        if log_folder:
            err_lines = [
                f"=== SILVER/GOLD ERROR — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===",
                f"\nStage: Silver MERGE",
                f"  Table: SILVER_DB.silver_tt_video_production",
                f"  SQL file: sql/silver_merge_tt_production.sql",
                f"  Error: {type(e).__name__} {e}\n",
                f"  Traceback:\n{traceback.format_exc()}",
            ]
            write_section_log(log_folder, f"silver_gold_errors_{run_key}.log", "\n".join(err_lines) + "\n")
        _failure_ctx.update({
            "stage": "BigQuery silver MERGE (production)",
            "minio_files": [datasets_config["produksi"]["file_path"], datasets_config["produksi"]["watermark_path"]],
            "rollback_hint": (
                "MinIO archive + bronze append succeeded; only the silver(produksi) MERGE failed. "
                "The silver upsert is idempotent over bronze, so a plain re-run is safe "
                "without any MinIO rollback."
            ),
        })
        raise
    print("[SILVER] MERGE PRODUCTION DONE")
    emit("SILVER", "silver_merger", "Silver MERGE into SILVER_DB.silver_tt_video_production DONE",
         metrics={"table": "SILVER_DB.silver_tt_video_production"},
         source={"system": "BigQuery", "entity": "BRONZE_DB.bronze_video_production"},
         target=TGT_BQ_SILVER_PRODUCTION)

    # 5) Gold: fact_performa_video_daily
    print("[GOLD] Building fact_performa_video_daily ...")
    try:
        create_gold_fact_video_performa_daily()
    except Exception as e:
        emit(
            "GOLD", "gold_builder",
            f"Gold build failed: {type(e).__name__} {e}",
            level="ERROR",
            metrics={"table": "GOLD_DB.fact_video_performa_daily"},
            source={"system": "BigQuery",
                    "entity": "SILVER_DB.silver_tt_video, SILVER_DB.silver_tt_video_production"},
            target=TGT_GOLD,
        )
        if log_folder:
            err_lines = [
                f"=== SILVER/GOLD ERROR — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===",
                f"\nStage: Gold build",
                f"  Table: GOLD_DB.fact_video_performa_daily",
                f"  Error: {type(e).__name__} {e}\n",
                f"  Traceback:\n{traceback.format_exc()}",
            ]
            write_section_log(log_folder, f"silver_gold_errors_{run_key}.log", "\n".join(err_lines) + "\n")
        _failure_ctx.update({
            "stage": "BigQuery gold build (fact_video_performa_daily)",
            "minio_files": [
                datasets_config["video"]["file_path"], datasets_config["video"]["watermark_path"],
                datasets_config["produksi"]["file_path"], datasets_config["produksi"]["watermark_path"],
            ],
            "rollback_hint": (
                "Both bronze datasets are already committed; only the final gold build failed. "
                "Gold is fully rebuilt from silver each run (CREATE OR REPLACE), so a plain "
                "re-run is safe without any MinIO rollback."
            ),
        })
        raise
    print("[GOLD] Load to GOLD_DB.fact_video_performa_daily DONE")
    emit("GOLD", "gold_builder",
         "Gold load to GOLD_DB.fact_video_performa_daily DONE",
         metrics={"table": "GOLD_DB.fact_video_performa_daily"},
         source={"system": "BigQuery",
                 "entity": "SILVER_DB.silver_tt_video, SILVER_DB.silver_tt_video_production"},
         target=TGT_GOLD)

    emit("FINISH", "etl_pipeline", "ETL Performa Video DONE",
         metrics={"rows_loaded": loaded_rows},
         source={"system": "Google_Sheets", "entity": "video, produksi"},
         target=TGT_GOLD)

    _finish(
        dataset_watermarks,
        "\n== ETL Performa Video DONE ==",
        status="success — full load",
        dry_run=dry_run,
        run_key=run_key,
        watermark_updates=_merge_watermark_updates(*dataset_watermark_updates.values()),
        bq_updates=_bq_full_load(loaded_rows),
        datasets_config=datasets_config,
    )


if __name__ == "__main__":
    run_daily_etl()
