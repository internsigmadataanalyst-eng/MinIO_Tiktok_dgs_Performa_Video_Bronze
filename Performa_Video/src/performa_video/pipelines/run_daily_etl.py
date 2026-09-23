# src/performa_video/pipelines/run_daily_etl.py

import io
import os
import traceback
from datetime import datetime

import pandas as pd

from src.performa_video.pipelines.config import (
    PROJECT_ID,
    SRC_MINIO,
    TGT_BQ_SILVER_VIDEO,
    TGT_BQ_SILVER_PRODUCTION,
    TGT_GOLD,
    WHITELIST_SHEETS,
    BQ_TARGETS,
    _failure_ctx,
)
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
    merge_to_silver_production,
)
from src.performa_video.transform.create_gold import create_gold_fact_video_performa_daily

from src.performa_video.utils.gsheet_client import get_gspread_client, init_spreadsheet_objects
from src.performa_video.utils.minio_client import (
    get_minio_client,
    get_sheet_watermarks,
    update_sheet_watermarks,
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
    effective_watermark_changes,
    build_drift_rows,
    merge_watermark_updates,
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
    write_wm_log,
    write_failure_log,
)
from src.performa_video.utils.notify import (
    send_alert_email,
    build_gate_abort_email,
    build_quarantine_email,
    build_recovery_email,
    QUARANTINE_SAMPLE_ROWS,
    QUARANTINE_SAMPLE_COLUMNS,
    bq_full_load,
    finish,
)
from src.performa_video.utils.bq_client import (
    get_credentials,
    fetch_existing_bronze_hashes,
)
from src.performa_video.utils.recovery import select_recovered


def run_daily_etl(dry_run: bool | None = None):
    print("== Start ETL Performa Video ==")

    if dry_run is None:
        dry_run = os.getenv("ETL_DRY_RUN", "0").strip().lower() in {"1", "true", "yes", "y"}

    if dry_run:
        print("[DRY-RUN] Mode aktif: TIDAK ada data yang ditulis ke MinIO/BigQuery/Silver.")

    _failure_ctx.clear()
    _failure_ctx.update({
        "stage": "", "minio_files": [], "rollback_hint": "",
    })

    gc = get_gspread_client()
    creds = get_credentials()
    minio_client, minio_bucket = get_minio_client()

    now_obj = datetime.now()
    today_key = now_obj.strftime("%Y%m%d")
    run_key = now_obj.strftime("%Y%m%d%H%M")
    log_folder = get_log_folder(run_key) if not dry_run else None

    if not is_event_logging_enabled():
        _, stop_event_logging = setup_event_logging(run_key, dry_run)
    else:
        stop_event_logging = None
    emit("PIPELINE", "run_daily_etl", "ETL start",
         metrics={"dry_run": dry_run},
         source={"system": "Google_Sheets", "entity": "video, produksi"},
         target=TGT_BQ_SILVER_VIDEO)

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
            write_wm_log(log_folder, run_key, video_status, produksi_status, video_sheet_passes,
                         f"ABORT — access errors on sheets: {error_sheets}")
            write_failure_log(
                log_folder, run_key,
                f"access errors on sheets: {error_sheets}",
            )
        return

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
            drift_rows=build_drift_rows(video_status),
        )
        send_alert_email(subject, body_html, dry_run=dry_run)
        if log_folder:
            write_wm_log(log_folder, run_key, video_status, produksi_status, video_sheet_passes,
                         f"ABORT — video sheets with no new data: {caught_up}")
            write_failure_log(
                log_folder, run_key,
                f"video sheets with no new data: {caught_up}",
            )
        return

    print("--- PRE-FLIGHT PASSED — video sheets have new data ---\n")

    if log_folder:
        video_pass_count = int(video_sheet_passes.sum())
        video_total = len(video_sheet_passes)
        produksi_behind = int(produksi_status["is_behind"].sum())
        produksi_total = len(produksi_status)
        verdict = (
            f"PASS — video: {video_pass_count}/{video_total} sheets, "
            f"produksi: {produksi_behind}/{produksi_total} akun"
        )
        write_wm_log(log_folder, run_key, video_status, produksi_status, video_sheet_passes, verdict)

    df_tt_vid_raw = fetch_tiktok_video(gc, spreadsheet_objects)
    df_tt_prod_raw = fetch_tiktok_produksi(gc, spreadsheet_objects)
    print(f"[INGEST] Rows raw video from GSheet: {len(df_tt_vid_raw)}")
    print(f"[INGEST] Rows raw produksi from GSheet: {len(df_tt_prod_raw)}")
    emit("INGEST", "gsheet_ingester", "Fetched raw rows from GSheets",
         metrics={"video_rows_raw": int(len(df_tt_vid_raw)),
                  "produksi_rows_raw": int(len(df_tt_prod_raw))},
         source={"system": "Google_Sheets", "entity": "video, produksi"})

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

        resolved = sync_error_manifest(minio_client, minio_bucket, df_error, v_report, today_key, run_key, subfolder=name, manifest_path=cfg["manifest_path"], fix_prefix=cfg["fix_prefix"], date_col=date_col, df_valid=df_valid, dry_run=dry_run, grain_col=cfg["grain_col"])

        if not df_error.empty:
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

        watermark_map, watermark_records = get_sheet_watermarks(
            minio_client, minio_bucket, watermark_path, grain_field=cfg["grain_field"]
        )
        dataset_watermarks[name] = watermark_records

        df_recovered = select_recovered(df_valid, resolved, v_report, date_col, grain_col=cfg["grain_col"])
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

        df_regular = df_valid.drop(df_recovered.index)
        df_filtered, sheet_max_dates = cfg["build_fn"](
            df_regular, sheet_watermarks=watermark_map
        )

        if df_recovered.empty:
            df_recovered_bronze = df_filtered.iloc[0:0]
        else:
            df_recovered_bronze, recovered_max_dates = cfg["build_fn"](
                df_recovered, sheet_watermarks={}
            )
            for key, max_date in recovered_max_dates.items():
                sheet_max_dates[key] = max(sheet_max_dates.get(key, max_date), max_date)

        dataset_watermark_updates[name] = sheet_max_dates.copy()

        df_filtered = pd.concat(
            [df_filtered, df_recovered_bronze], ignore_index=True
        ).drop_duplicates(subset=["row_hash_raw"])

        if not df_filtered.empty:
            batch_dates = df_filtered["tanggal"].dropna()
            min_date = str(batch_dates.min().date()) if not batch_dates.empty else None
            existing_hashes = fetch_existing_bronze_hashes(creds, table_id=cfg["bq_table_id"], project_id=PROJECT_ID, min_date=min_date)
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
            _failure_ctx.update({
                "stage": f"BigQuery bronze load (append) — {name}",
                "minio_files": [file_path, cfg["watermark_path"]],
                "rollback_hint": (
                    "Watermark NOT updated — a re-run will safely re-select "
                    f"rows for '{name}'. No manual rollback needed."
                ),
            })
            raise
        print(f"[BRONZE] Load to {cfg['bq_table_id']} DONE")
        loaded_rows[name] = int(len(df_filtered))
        emit("LOAD", "bigquery_loader", f"[{name}] Bronze load to {cfg['bq_table_id']} DONE",
             metrics={"dataset": name, "rows_loaded": int(len(df_filtered)),
                      "table": f"{PROJECT_ID}.{cfg['bq_table_id']}"},
             source=src_wm, target=tgt_bq)

        update_sheet_watermarks(
            minio_client, minio_bucket, watermark_path, watermark_records, sheet_max_dates,
            grain_field=cfg["grain_field"],
        )

    if dry_run:
        print("[DRY-RUN] Akan MERGE ke SILVER_DB.silver_tt_video + SILVER_DB.silver_tt_video_production + build GOLD")
        emit("FINISH", "etl_pipeline", "ETL Performa Video DONE (DRY-RUN)",
             metrics={"rows_loaded": loaded_rows},
             source={"system": "Google_Sheets", "entity": "video, produksi"},
             target=TGT_GOLD)
        finish(
            dataset_watermarks,
            "== ETL Performa Video DONE (DRY-RUN) ==",
            status="dry-run — full load (would run)",
            dry_run=dry_run,
            run_key=run_key,
            watermark_updates=merge_watermark_updates(*dataset_watermark_updates.values()),
            bq_updates=bq_full_load(loaded_rows),
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

    finish(
        dataset_watermarks,
        "\n== ETL Performa Video DONE ==",
        status="success — full load",
        dry_run=dry_run,
        run_key=run_key,
        watermark_updates=merge_watermark_updates(*dataset_watermark_updates.values()),
        bq_updates=bq_full_load(loaded_rows),
        datasets_config=datasets_config,
    )


if __name__ == "__main__":
    run_daily_etl()
