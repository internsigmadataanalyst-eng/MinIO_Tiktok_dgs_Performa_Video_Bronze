# src/performa_video/utils/minio_client.py
import os
import json
import io
from datetime import datetime, date
from minio import Minio
from minio.error import S3Error
import pandas as pd


def get_minio_client() -> tuple[Minio, str]:
    """Instantiates and returns the MinIO client alongside the configured bucket name."""
    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")
    minio_bucket = os.getenv("MINIO_BUCKET")
    minio_secure = os.getenv("MINIO_SECURE", "false").lower() == "true"

    client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=minio_secure,
    )
    return client, minio_bucket


def get_sheet_watermarks(minio_client: Minio, bucket: str, watermark_path: str, grain_field: str = "toko") -> tuple[dict, list]:
    """Fetches the per-sheet watermark table from MinIO.

    Watermark grain is (creds, sheet_name, grain) — the third component is stored
    in the field named by `grain_field` verbatim, no normalization. For video it
    is 'toko' (holds toko); for produksi it is 'akun' (holds akun). For backward
    compat with legacy records written under 'toko', the 'toko' field is used as
    a fallback when broadcast using the newer grain_field name.
    Assumes the watermark file holds the per-sheet table format
    ({"sheets": [{"creds","sheet_name","<grain_field>","last_processed_date","updated_at"}]}).

    Returns:
        watermark_map: {(creds, sheet_name, grain): last_processed_date}
        records: raw rows as read.
    """
    try:
        minio_client.stat_object(bucket, watermark_path)
    except S3Error as e:
        if e.code in ["NoSuchKey", "AccessDenied"]:
            return {}, []
        raise e

    response = minio_client.get_object(bucket, watermark_path)
    data = json.loads(response.read().decode("utf-8"))
    response.close()
    response.release_conn()

    records = data["sheets"]

    watermark_map = {}
    for rec in records:
        creds = str(rec["creds"])
        sheet_name = str(rec.get("sheet_name") or "")
        toko = str(rec.get(grain_field) or rec.get("toko") or "")  # verbatim grain value
        date_val = str(rec["last_processed_date"]).strip()[:10]
        key = (creds, sheet_name, toko)
        watermark_map[key] = max(watermark_map.get(key, ""), date_val)

    print(f"[MINIO] Watermark found for {len(records)} group(s) (creds,sheet_name,toko).")
    return watermark_map, records


def update_sheet_watermarks(minio_client: Minio, bucket: str, watermark_path: str, prev_records: list, sheet_max_dates: dict, creds_sheet_map: dict | None = None, grain_field: str = "toko"):
    """Rebuilds the watermark table preserving idle (creds,sheet_name,grain) groups.

    The grain value (toko for video, akun for produksi) is stored in the JSON
    field named by `grain_field`.
    """
    now = datetime.now().isoformat()

    by_key = {}
    for rec in prev_records:
        creds = str(rec.get("creds") or "")
        sheet_name = str(rec.get("sheet_name") or "")
        toko = str(rec.get(grain_field) or rec.get("toko") or "")
        record = {
            "creds": creds,
            "sheet_name": sheet_name or creds,
            "last_processed_date": str(rec.get("last_processed_date") or "").strip()[:10],
            "updated_at": rec.get("updated_at") or now,
        }
        record[grain_field] = toko
        by_key[(creds, sheet_name, toko)] = record

    for (creds, sheet_name, toko), max_date in sheet_max_dates.items():
        creds = str(creds)
        sheet_name = str(sheet_name or "")
        toko = str(toko or "")
        record = {
            "creds": creds,
            "sheet_name": sheet_name or creds,
            "last_processed_date": str(max_date).strip()[:10],
            "updated_at": now,
        }
        record[grain_field] = toko
        by_key[(creds, sheet_name, toko)] = record

    records = sorted(
        by_key.values(),
        key=lambda rec: (
            str(rec["creds"]),
            str(rec["sheet_name"]),
            str(rec.get(grain_field) or rec.get("toko") or ""),
        ),
    )

    payload = json.dumps({"sheets": records}).encode("utf-8")
    minio_client.put_object(
        bucket,
        watermark_path,
        io.BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )
    print(f"[MINIO] Updated watermark JSON for {len(records)} group(s) (creds,sheet_name,toko).")


ERROR_MANIFEST_PATH = "error_list_watermark/error_manifest.json"
FIX_MANIFEST_PREFIX = "fix_error_list_watermark"
QUARANTINE_PREFIX = "quarantine"


def write_quarantine(minio_client: Minio, bucket: str, df_error: pd.DataFrame, today_key: str, run_key: str, subfolder: str = ""):
    """Saves bad rows to MinIO under quarantine/[subfolder/]date=YYYYMMDD/<run_key>.parquet."""
    if df_error.empty:
        return

    sub = f"{subfolder}/" if subfolder else ""
    folder_path = f"{QUARANTINE_PREFIX}/{sub}date={today_key}/"
    file_path = f"{folder_path}quarantine_{run_key}.parquet"

    minio_client.put_object(bucket, folder_path, io.BytesIO(b""), length=0)

    parquet_bytes = df_error.to_parquet(index=False, engine="pyarrow")
    minio_client.put_object(
        bucket,
        file_path,
        io.BytesIO(parquet_bytes),
        length=len(parquet_bytes),
        content_type="application/octet-stream",
    )
    print(f"[MINIO] Quarantine bad rows to: {file_path}")


def _error_date_series(df: pd.DataFrame) -> pd.Series:
    """Parses the Tanggal column into ISO date strings for error grouping.

    Unparseable dates become the literal 'INVALID_DATE' so they still form a
    stable group key. Shared by filter_already_quarantined (the dedupe gate)
    and sync_error_manifest so both use EXACTLY the same matching grain.
    """
    from src.performa_video.utils.transform_utils import parse_mixed_dates

    if "Tanggal" in df.columns:
        parsed = parse_mixed_dates(df["Tanggal"], return_date=False)
        error_date = parsed.dt.date.astype(str)
        return error_date.where(parsed.notna(), "INVALID_DATE")
    return pd.Series("INVALID_DATE", index=df.index)


def _grain_col(df: pd.DataFrame, grain_col: str):
    """Returns the actual column name matching `grain_col` case-insensitively, or None.

    Raw GSheet frames use capitalized names (Toko, Akun) before build_bronze_* runs
    to_snake_case; snake_cased frames use lowercase. Matching case-insensitively keeps
    the watermark/error grain consistent across both representations.
    """
    if grain_col in df.columns:
        return grain_col
    for c in df.columns:
        if str(c).strip().lower() == grain_col.lower():
            return c
    return None


def _grain_series(df: pd.DataFrame, grain_col: str) -> pd.Series:
    """Returns the grain values (verbatim, empty for missing) for a column.

    `grain_col` is the snake_case column name to group by (e.g. 'toko' for video,
    'akun' for produksi). Matched case-insensitively; falls back to empty strings
    when the column is absent.
    """
    col = _grain_col(df, grain_col)
    if col is not None:
        return df[col].astype(str).str.strip()
    return pd.Series("", index=df.index, dtype=str)


def filter_already_quarantined(minio_client: Minio, bucket: str, df_error: pd.DataFrame, manifest_path: str = ERROR_MANIFEST_PATH, grain_col: str = "toko") -> pd.DataFrame:
    """Dedupe gate BEFORE writing quarantine: drops already-quarantined bad rows.

    Compares df_error against the manifest state of the LAST run. A group
    (sheet_name, creds, grain, error_date) is skipped ONLY when an open manifest entry
    exists with the same key AND the same n_rows. New tanggal, new sheet, new grain,
    or a changed row count pass through in full and get re-quarantined.

    MUST be called BEFORE sync_error_manifest: that function writes this run's
    groups into the manifest, so calling it after would make every group look
    like a duplicate and nothing would ever be quarantined.
    """
    if df_error is None or df_error.empty:
        return df_error

    try:
        minio_client.stat_object(bucket, manifest_path)
        response = minio_client.get_object(bucket, manifest_path)
        data = json.loads(response.read().decode("utf-8"))
        response.close()
        response.release_conn()
        open_records = [r for r in data.get("errors", []) if r.get("status") == "open"]
    except S3Error as e:
        if e.code in ["NoSuchKey", "AccessDenied"]:
            return df_error
        raise e

    known = {}
    for rec in open_records:
        key = (str(rec.get("sheet_name")), str(rec.get("creds")), str(rec.get("toko") or ""), str(rec.get("error_date")))
        try:
            known[key] = int(rec.get("n_rows") or 0)
        except (TypeError, ValueError):
            known[key] = 0

    df = df_error.copy()
    df["_error_date"] = _error_date_series(df)
    sn_col = "sheet_name" if "sheet_name" in df.columns else "creds"
    cr_col = "creds" if "creds" in df.columns else sn_col
    toko_col = _grain_col(df, grain_col)

    keep = pd.Series(True, index=df.index)
    n_skip_groups = 0
    if toko_col is not None:
        group_cols = [sn_col, cr_col, toko_col, "_error_date"]
    else:
        group_cols = [sn_col, cr_col, "_error_date"]

    for keys, idx in df.groupby(group_cols, dropna=False).groups.items():
        if toko_col is not None:
            sheet_name, creds, toko, error_date = keys
            toko = "" if pd.isna(toko) else str(toko)
        else:
            sheet_name, creds, error_date = keys
            toko = ""
        key = (str(sheet_name), str(creds), str(toko), str(error_date))
        n_rows = len(idx)
        if known.get(key) == n_rows:
            keep.loc[idx] = False
            n_skip_groups += 1

    filtered = df.loc[keep].drop(columns=["_error_date"])
    skipped = len(df) - int(keep.sum())
    print(
        f"[QUARANTINE GATE] {int(keep.sum())} new row(s) -> quarantine | "
        f"skipped {skipped} duplicate row(s) in {n_skip_groups} group(s)"
    )
    return filtered


def _confirmed_recovered_keys(df_valid: pd.DataFrame, candidates: list, date_col: str = "Tanggal", grain_col: str = "toko") -> set:
    """Which candidate keys (sheet_name, creds, grain, error_date) are PROVEN recovered.

    A key is only confirmed when the same number of valid rows now exist as the
    manifest's n_rows for that key. Absence from df_error alone is NOT proof of
    recovery (an error can change signature, e.g. numeric -> date), so unconfirmed
    keys are kept open instead of being falsely marked fixed.
    Grain verbatim, empty for produksi when the column is absent.
    """
    if df_valid is None or df_valid.empty or not candidates:
        return set()

    toko_series = _grain_series(df_valid, grain_col)

    try:
        tanggal_str = pd.to_datetime(df_valid[date_col]).dt.date.astype(str)
    except Exception:
        tanggal_str = df_valid[date_col].astype(str)

    key_series = (
        df_valid["sheet_name"].astype(str)
        + "|" + df_valid["creds"].astype(str)
        + "|" + toko_series
        + "|" + tanggal_str
    )
    counts = key_series.value_counts()

    confirmed = set()
    for rec in candidates:
        key = f'{rec["sheet_name"]}|{rec["creds"]}|{rec.get("toko") or ""}|{rec["error_date"]}'
        n_expected = int(rec.get("n_rows") or 0)
        if n_expected > 0 and counts.get(key, 0) == n_expected:
            confirmed.add((str(rec["sheet_name"]), str(rec["creds"]), str(rec.get("toko") or ""), str(rec["error_date"])))
    return confirmed


def _is_date_future_entry(rec: dict) -> bool:
    """True when a manifest entry carries the 'date_future' error reason.

    A future-date wrong input in the primary date column, once corrected, ALWAYS
    changes its error_date key (the row moves to the corrected date), so the strict
    count-match recovery can never resolve it. For this specific error type, "no
    longer present in this run's errors" is reliable proof of remediation (the date
    was corrected or the row removed). Mixed-reason groups still resolve on
    'date_future' being present, since the future-date defect was remediated.
    """
    reasons = rec.get("error_reasons") or []
    return "date_future" in reasons


def _is_legacy_future_date(rec: dict) -> bool:
    """True for a one-time legacy entry created BEFORE date_future tagging.

    Such entries lack the 'error_reasons' field, so we fall back to a heuristic:
    if the stored error_date parses to a date strictly after today, it is a stale
    future-date artifact. Only applied when the field is absent (None), so already-
    tagged entries are never double-classified. Once written, every entry carries
    'error_reasons', making this effectively a one-time cleanup pass.
    """
    reasons = rec.get("error_reasons")
    if reasons is not None:
        return False
    d = rec.get("error_date")
    try:
        return pd.to_datetime(d).date() > date.today()
    except Exception:
        return False


def sync_error_manifest(minio_client: Minio, bucket: str, df_error: pd.DataFrame, report: dict, today_key: str, run_key: str, subfolder: str = "", manifest_path: str = ERROR_MANIFEST_PATH, fix_prefix: str = FIX_MANIFEST_PREFIX, date_col: str = "Tanggal", df_valid: pd.DataFrame = None, dry_run: bool = False, grain_col: str = "toko"):
    """Syncs the error manifest at error_list_watermark/error_manifest.json.

    Grain is (sheet_name, creds, grain, error_date) — same as watermark (creds,sheet_name,grain) plus error_date, grain="" when the column is absent.
    Runs EVERY run (even with empty df_error):
      1. Reads current open entries.
      2. Builds current-run entries from df_error grouped by
         (sheet_name, creds, toko, error_date) — the same grain as the watermark.
      3. Resolves only entries whose key is no longer detected this run AND is
         PROVEN recovered (matching rows exist in df_valid with count == n_rows),
         OR carries a 'date_future' error reason (correcting a wrong date changes
         its key, so count-match can never fire; absence this run is proof of
         remediation), OR is a legacy pre-tagging entry whose stored error_date is
         in the future (one-time cleanup). Resolved entries are removed from the
         manifest and written as fix records to
         fix_error_list_watermark/<subfolder>/date=YYYYMMDD/fix_<run_key>.json.
      4. Refreshes open entries still detected this run with the latest
         n_rows / affected_columns / path, and appends new open entries not
         already present.
      5. Writes the manifest back only if something changed (avoids creating
         an empty file when there is nothing to do).

    Returns the list of confirmed resolved entries (sheet_name, creds, toko, error_date)
    so callers can re-load the recovered rows via PATH A (bypassing the watermark).
    """
    if df_valid is None:
        df_valid = df_error.iloc[0:0]
    from src.performa_video.utils.transform_utils import parse_mixed_dates

    now = datetime.now().isoformat()
    sub = f"{subfolder}/" if subfolder else ""
    quarantine_path = f"{QUARANTINE_PREFIX}/{sub}date={today_key}/quarantine_{run_key}.parquet"

    current_entries = []
    if not df_error.empty:
        df_error = df_error.copy()
        if date_col in df_error.columns:
            df_error["_parsed_date"] = parse_mixed_dates(df_error[date_col], return_date=False)
            df_error["_error_date"] = df_error["_parsed_date"].dt.date.astype(str)
            df_error["_error_date"] = df_error["_error_date"].where(
                df_error["_parsed_date"].notna(), "INVALID_DATE"
            )
        else:
            df_error["_error_date"] = "INVALID_DATE"

        sn_col = "sheet_name" if "sheet_name" in df_error.columns else "creds"
        cr_col = "creds" if "creds" in df_error.columns else sn_col
        toko_col = _grain_col(df_error, grain_col)

        if toko_col is not None:
            group_cols = [sn_col, cr_col, toko_col, "_error_date"]
        else:
            group_cols = [sn_col, cr_col, "_error_date"]

        for keys, idx in df_error.groupby(group_cols, dropna=False).groups.items():
            if toko_col is not None:
                sheet_name, creds, toko, error_date = keys
                toko = "" if pd.isna(toko) else str(toko)
            else:
                sheet_name, creds, error_date = keys
                toko = ""
            bad_vals = set()
            for col in report["affected_columns"]:
                if col not in df_error.columns:
                    continue
                vals = df_error.loc[idx, col].astype(str).str.strip()
                bad_vals.update(
                    v for v in vals
                    if v and v.lower() not in ("nan", "none", "-", "n/a", "nat")
                )
            current_entries.append({
                "sheet_name": str(sheet_name),
                "creds": str(creds),
                "toko": str(toko),
                "error_date": str(error_date),
                "affected_columns": list(report["affected_columns"]),
                "bad_values": sorted(bad_vals),
                "error_reasons": sorted({
                    str(r) for r in df_error.loc[idx, "error_reason"].astype(str).str.split("|").explode()
                }) if "error_reason" in df_error.columns else [],
                "n_rows": int(len(idx)),
                "reported_at": now,
                "path": quarantine_path,
                "status": "open",
            })

    manifest_existed = True
    try:
        minio_client.stat_object(bucket, manifest_path)
        response = minio_client.get_object(bucket, manifest_path)
        data = json.loads(response.read().decode("utf-8"))
        response.close()
        response.release_conn()
        open_records = [r for r in data.get("errors", []) if r.get("status") == "open"]
    except S3Error as e:
        if e.code in ["NoSuchKey", "AccessDenied"]:
            open_records = []
            manifest_existed = False
        else:
            raise e

    current_keys = {(e["sheet_name"], e["creds"], e.get("toko") or "", e["error_date"]) for e in current_entries}

    # Only resolve keys that are PROVEN recovered (same count of valid rows as the
    # manifest n_rows). Absence from df_error is not proof: an error can change
    # signature (e.g. numeric -> date) and would otherwise be falsely marked fixed,
    # destroying the recovery hook for a later real fix.
    confirmed = _confirmed_recovered_keys(df_valid, [
        {
            "sheet_name": str(rec.get("sheet_name")),
            "creds": str(rec.get("creds")),
            "toko": str(rec.get("toko") or ""),
            "error_date": str(rec.get("error_date")),
            "n_rows": rec.get("n_rows"),
        }
        for rec in open_records
        if (str(rec.get("sheet_name")), str(rec.get("creds")), str(rec.get("toko") or ""), str(rec.get("error_date"))) not in current_keys
    ], date_col=date_col, grain_col=grain_col)

    resolved = []
    refreshed = {}
    n_legacy_resolved = 0
    for rec in open_records:
        key = (str(rec.get("sheet_name")), str(rec.get("creds")), str(rec.get("toko") or ""), str(rec.get("error_date")))
        if key in current_keys:
            refreshed[key] = rec
        elif key in confirmed or _is_date_future_entry(rec):
            resolved.append(rec)
        elif _is_legacy_future_date(rec):
            resolved.append(rec)
            n_legacy_resolved += 1
        else:
            refreshed[key] = rec  # unconfirmed -> keep open

    # Fresh current-run entries always win: refresh n_rows / affected_columns /
    # reported_at / path for keys that already exist, append brand-new keys.
    # Preserve original bad_values via union so historical values are never lost.
    for entry in current_entries:
        key = (entry["sheet_name"], entry["creds"], entry["toko"], entry["error_date"])
        if key in refreshed:
            old_bv = set(refreshed[key].get("bad_values", []))
            new_bv = set(entry.get("bad_values", []))
            entry["bad_values"] = sorted(old_bv | new_bv)
        refreshed[key] = entry

    remaining = list(refreshed.values())

    # Detect changes by comparing serialized content so refreshed entries (same
    # key, different n_rows / columns) are written back, not just appended ones.
    new_payload = json.dumps({"errors": remaining}, ensure_ascii=False, sort_keys=True)
    old_payload = json.dumps({"errors": open_records}, ensure_ascii=False, sort_keys=True)
    changed = bool(resolved) or new_payload != old_payload
    if not changed and not manifest_existed:
        return []

    if resolved:
        fix_folder = f"{fix_prefix}/date={today_key}/"
        fix_path = f"{fix_folder}fix_{run_key}.json"
        fixes = [{
            "sheet_name": r["sheet_name"],
            "creds": r["creds"],
            "toko": r.get("toko") or "",
            "error_date": r["error_date"],
            "affected_columns": r.get("affected_columns", []),
            "bad_values": r.get("bad_values", []),
            "resolved_at": now,
            "path": r.get("path", ""),
            "status": "fixed",
        } for r in resolved]
        if dry_run:
            print(f"[DRY-RUN] Akan tulis fix record {len(fixes)} error entr(y/ies) -> {fix_path}")
        else:
            minio_client.put_object(bucket, fix_folder, io.BytesIO(b""), length=0)
            payload = json.dumps({"fixes": fixes}, ensure_ascii=False).encode("utf-8")
            minio_client.put_object(
                bucket,
                fix_path,
                io.BytesIO(payload),
                length=len(payload),
                content_type="application/json",
            )
            print(f"[MINIO] Resolved {len(fixes)} error entr(y/ies) -> fix record: {fix_path}")

    if n_legacy_resolved:
        print(f"[MINIO] Cleaned {n_legacy_resolved} legacy future-date error entr(y/ies)")

    if dry_run:
        if current_entries:
            print(f"[DRY-RUN] Akan sync {len(current_entries)} open error entr(y/ies) ke {manifest_path}")
    else:
        payload = json.dumps({"errors": remaining}, ensure_ascii=False).encode("utf-8")
        minio_client.put_object(
            bucket,
            manifest_path,
            io.BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )
        if current_entries:
            print(f"[MINIO] Synced {len(current_entries)} open error entr(y/ies) to {manifest_path}")
    return resolved


def filter_by_sheet_watermark(df: pd.DataFrame, creds_col: str, sheet_name_col: str, toko_col: str, date_col: str, watermarks: dict) -> tuple[pd.DataFrame, dict]:
    """Per-(creds,sheet_name,grain) incremental filter on already-clean dates (Timestamp dtype).

    Grain is (creds, sheet_name, grain) — grain verbatim, empty when the column is absent.
    For each group key (creds, sheet_name, grain), keep rows where date > watermarks[key].
    Groups without a watermark are treated as full load.
    Returns (filtered_df, sheet_max_dates) where sheet_max_dates maps
    (creds, sheet_name, grain) -> last processed date (ISO).
    The grain column is configurable (toko for video, akun for produksi).
    """
    parsed = pd.to_datetime(df[date_col])

    if creds_col not in df.columns or sheet_name_col not in df.columns or toko_col not in df.columns:
        # produksi: toko missing → toko=""
        if toko_col not in df.columns and creds_col in df.columns and sheet_name_col in df.columns:
            keep = pd.Series(True, index=df.index)
            for (creds_val, sheet_val), idx in df.groupby([creds_col, sheet_name_col], dropna=False).groups.items():
                creds_key = "" if pd.isna(creds_val) else str(creds_val)
                sheet_key = "" if pd.isna(sheet_val) else str(sheet_val)
                wm = watermarks.get((creds_key, sheet_key, ""))
                if wm:
                    cutoff = pd.Timestamp(wm)
                    keep.loc[idx] = parsed.loc[idx] > cutoff
            filtered = df[keep].copy()
            sheet_max_dates = {}
            parsed_kept = parsed[keep]
            for (creds_val, sheet_val), idx in filtered.groupby([creds_col, sheet_name_col], dropna=False).groups.items():
                mx = parsed_kept.loc[idx].dropna()
                if not mx.empty:
                    creds_key = "" if pd.isna(creds_val) else str(creds_val)
                    sheet_key = "" if pd.isna(sheet_val) else str(sheet_val)
                    sheet_max_dates[(creds_key, sheet_key, "")] = mx.max().date().isoformat()
            return filtered, sheet_max_dates
        cols = [c for c in [creds_col, sheet_name_col, toko_col] if c in df.columns]
        if not cols:
            return df.copy(), {}
        keep = pd.Series(True, index=df.index)
        for keys, idx in df.groupby(cols, dropna=False).groups.items():
            if not isinstance(keys, tuple):
                keys = (keys,)
            creds_k = str(keys[0]) if len(keys) > 0 and pd.notna(keys[0]) else ""
            sheet_k = str(keys[1]) if len(keys) > 1 and pd.notna(keys[1]) else ""
            toko_k = str(keys[2]) if len(keys) > 2 and pd.notna(keys[2]) else ""
            wm = watermarks.get((creds_k, sheet_k, toko_k))
            if wm:
                cutoff = pd.Timestamp(wm)
                keep.loc[idx] = parsed.loc[idx] > cutoff
        filtered = df[keep].copy()
        sheet_max_dates = {}
        parsed_kept = parsed[keep]
        for keys, idx in filtered.groupby(cols, dropna=False).groups.items():
            mx = parsed_kept.loc[idx].dropna()
            if not mx.empty:
                if not isinstance(keys, tuple):
                    keys = (keys,)
                creds_k = str(keys[0]) if len(keys) > 0 and pd.notna(keys[0]) else ""
                sheet_k = str(keys[1]) if len(keys) > 1 and pd.notna(keys[1]) else ""
                toko_k = str(keys[2]) if len(keys) > 2 and pd.notna(keys[2]) else ""
                sheet_max_dates[(creds_k, sheet_k, toko_k)] = mx.max().date().isoformat()
        return filtered, sheet_max_dates

    keep = pd.Series(True, index=df.index)
    for (creds_val, sheet_val, toko_val), idx in df.groupby([creds_col, sheet_name_col, toko_col], dropna=False).groups.items():
        creds_key = "" if pd.isna(creds_val) else str(creds_val)
        sheet_key = "" if pd.isna(sheet_val) else str(sheet_val)
        toko_key = "" if pd.isna(toko_val) else str(toko_val)
        wm = watermarks.get((creds_key, sheet_key, toko_key))
        if wm:
            cutoff = pd.Timestamp(wm)
            keep.loc[idx] = parsed.loc[idx] > cutoff

    filtered = df[keep].copy()

    sheet_max_dates = {}
    parsed_kept = parsed[keep]
    for (creds_val, sheet_val, toko_val), idx in filtered.groupby([creds_col, sheet_name_col, toko_col], dropna=False).groups.items():
        mx = parsed_kept.loc[idx].dropna()
        if not mx.empty:
            creds_key = "" if pd.isna(creds_val) else str(creds_val)
            sheet_key = "" if pd.isna(sheet_val) else str(sheet_val)
            toko_key = "" if pd.isna(toko_val) else str(toko_val)
            sheet_max_dates[(creds_key, sheet_key, toko_key)] = mx.max().date().isoformat()

    return filtered, sheet_max_dates
