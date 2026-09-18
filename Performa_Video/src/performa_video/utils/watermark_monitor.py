# src/performa_video/utils/watermark_monitor.py
"""Watermark drift monitor — pre-flight gate for the daily ETL.

Compares the live GSheet state against the stored MinIO watermark to decide
whether each sheet has new data worth processing.

Gate rule: every video sheet (matz..imam) must have at least 1 toko group
where live tanggal > watermark date; produksi must have at least 1 akun behind.
If any sheet has 0 grain groups behind (or an access error), the ETL aborts.
"""
import gspread
import pandas as pd

from src.performa_video.utils.gsheet_client import with_retry_on_429
from src.performa_video.utils.minio_client import get_sheet_watermarks
from src.performa_video.utils.transform_utils import parse_mixed_dates, to_snake_case


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def col_index_to_letter(idx: int) -> str:
    """0-indexed column position -> spreadsheet column letter (0 -> 'A', 26 -> 'AA')."""
    letter = ""
    idx += 1
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        letter = chr(65 + rem) + letter
    return letter


# Backwards-compatible alias for the shared 429 retry helper.
def with_retry(func, *args, max_retries=4, base_delay=15, **kwargs):
    """Retry wrapper with linear backoff for Google Sheets API 429 rate limits."""
    return with_retry_on_429(func, *args, max_retries=max_retries, delay=base_delay, **kwargs)


def get_df_minimal(
    sh: gspread.Spreadsheet,
    actual_sheet_name: str,
    tanggal_col: str,
    toko_col: str | None,
    required_cols: list[str] | None,
    header_row: int = 2,
    data_start_row: int = 3,
) -> pd.DataFrame:
    """Fetch ONLY the columns actually needed from a Google Sheet.

    tanggal_col / toko_col / required_cols must be passed as snake_case
    (matching how to_snake_case normalizes the real header).

    Uses a single batch_get call for all needed columns — one network round-trip.
    """
    ws = with_retry_on_429(sh.worksheet, actual_sheet_name)
    header_raw = with_retry(ws.row_values, header_row + 1)
    header_norm = [to_snake_case(c) for c in header_raw]

    needed = [tanggal_col] + ([toko_col] if toko_col else []) + list(required_cols or [])
    needed = list(dict.fromkeys(needed))  # dedupe, preserve order

    ranges = []
    for name in needed:
        if name not in header_norm:
            raise KeyError(f"column '{name}' not found. Available: {header_norm}")
        col_letter = col_index_to_letter(header_norm.index(name))
        ranges.append(f"{col_letter}{data_start_row + 1}:{col_letter}")

    results = with_retry(ws.batch_get, ranges, value_render_option="UNFORMATTED_VALUE")

    columns_data = {}
    for name, col_values in zip(needed, results):
        columns_data[name] = [row[0] if row else "" for row in col_values]

    max_len = max((len(v) for v in columns_data.values()), default=0)
    for name in columns_data:
        columns_data[name] += [""] * (max_len - len(columns_data[name]))

    return pd.DataFrame(columns_data)


def get_max_tanggal_by_toko(
    df: pd.DataFrame,
    toko_col: str | None = "toko",
    tanggal_col: str = "tanggal",
    required_cols: list[str] | None = None,
) -> pd.Series:
    """Group by toko (or overall if toko_col is None) and return max tanggal.

    Rows where required_cols are blank are excluded before computing the max.
    Returns a Series like {toko_value: max_tanggal}.
    """
    tmp = df.copy()
    tmp[tanggal_col] = parse_mixed_dates(tmp[tanggal_col], return_date=False)

    if required_cols:
        for col in required_cols:
            if col not in tmp.columns:
                raise KeyError(f"required_cols column '{col}' not found. Available: {list(tmp.columns)}")
            tmp = tmp[tmp[col].astype(str).str.strip().replace({"nan": ""}) != ""]

    if toko_col is None:
        return pd.Series({"_all": tmp[tanggal_col].max()})

    return tmp.groupby(toko_col)[tanggal_col].max()


# ---------------------------------------------------------------------------
# Core engine
# ---------------------------------------------------------------------------

def compare_watermark_vs_sheet(
    logical_name: str,
    registry_keys: list[str],
    sheet_registry: dict,
    watermark_records: list[dict],
    spreadsheet_objects: dict,
    toko_col: str | None = "toko",
    tanggal_col: str = "tanggal",
    required_cols: list[str] | None = None,
    header_row: int = 2,
    data_start_row: int = 3,
) -> pd.DataFrame:
    """Compare stored watermark against live GSheet state per sheet_name.

    For each registry key, fetches the relevant columns from the live sheet,
    computes the max tanggal per toko (or overall), and compares against
    the watermark's last_processed_date.

    Returns a DataFrame with columns:
        sheet_name, grain, logical_sheet, sheet_max_tanggal,
        last_processed_date, is_behind, status
    """
    rows = []
    for registry_key in registry_keys:
        if registry_key not in sheet_registry:
            rows.append({
                "sheet_name": registry_key, "grain": None, "logical_sheet": logical_name,
                "sheet_max_tanggal": None, "last_processed_date": None,
                "is_behind": False, "status": "registry_key not found in sheet_registry",
            })
            continue

        spreadsheet_key, worksheet_name = sheet_registry[registry_key]
        sh = spreadsheet_objects.get(spreadsheet_key)
        if sh is None:
            rows.append({
                "sheet_name": registry_key, "grain": None, "logical_sheet": logical_name,
                "sheet_max_tanggal": None, "last_processed_date": None,
                "is_behind": False, "status": "spreadsheet object not found",
            })
            continue

        try:
            df = get_df_minimal(
                sh, worksheet_name,
                tanggal_col=tanggal_col,
                toko_col=toko_col,
                required_cols=required_cols,
                header_row=header_row,
                data_start_row=data_start_row,
            )
            max_by_toko = get_max_tanggal_by_toko(df, toko_col, tanggal_col, required_cols)
        except Exception as e:
            rows.append({
                "sheet_name": registry_key, "grain": None, "logical_sheet": logical_name,
                "sheet_max_tanggal": None, "last_processed_date": None,
                "is_behind": False, "status": f"error: {e}",
            })
            continue

        wm_rows = [r for r in watermark_records if r.get("sheet_name") == registry_key]
        for wm_row in wm_rows:
            grain_val = wm_row.get(toko_col, wm_row.get("toko")) if toko_col is not None else None
            lookup_key = grain_val if toko_col is not None else "_all"
            sheet_max = max_by_toko.get(lookup_key, pd.NaT)
            last_processed = pd.to_datetime(wm_row.get("last_processed_date"))
            rows.append({
                "sheet_name": registry_key,
                "grain": grain_val,
                "logical_sheet": logical_name,
                "sheet_max_tanggal": sheet_max,
                "last_processed_date": last_processed,
                "is_behind": bool(pd.notna(sheet_max) and sheet_max > last_processed),
                "status": "ok" if pd.notna(sheet_max) else "no data found",
            })

    df_out = pd.DataFrame(rows)
    if "is_behind" not in df_out.columns:
        df_out["is_behind"] = pd.Series(dtype=bool)
    return df_out


# ---------------------------------------------------------------------------
# Project-specific wrappers
# ---------------------------------------------------------------------------

def performa_video_watermark_check(spreadsheet_objects: dict, minio_client, minio_bucket: str) -> pd.DataFrame:
    """Check watermark drift for the 5 video sheets (matz..imam) using toko grain."""
    watermark_records = get_sheet_watermarks(
        minio_client, minio_bucket, "watermarks/performa_video.json"
    )[1]
    sheet_registry = {
        "matz": ("SH_KEY_MATZ", "Performa Video"),
        "ian":  ("SH_KEY_IAN",  "Performa Video"),
        "deni": ("SH_KEY_DENI", "Performa Video"),
        "riwa": ("SH_KEY_RIWA", "Performa Video"),
        "imam": ("SH_KEY_IMAM", "Performa Video"),
    }
    return compare_watermark_vs_sheet(
        "Performa Video", list(sheet_registry.keys()), sheet_registry,
        watermark_records, spreadsheet_objects,
        toko_col="toko", tanggal_col="tanggal",
        required_cols=["tanggal"],
    )


def produksi_watermark_check(spreadsheet_objects: dict, minio_client, minio_bucket: str) -> pd.DataFrame:
    """Check watermark drift for produksi using akun grain."""
    watermark_records = get_sheet_watermarks(
        minio_client, minio_bucket, "watermarks/produksi.json", grain_field="akun"
    )[1]
    sheet_registry = {
        "produksi": ("SH_KEY_PRODUKSI", "DATABASE KONTEN CC E-COM"),
    }
    return compare_watermark_vs_sheet(
        "Produksi", list(sheet_registry.keys()), sheet_registry,
        watermark_records, spreadsheet_objects,
        toko_col="akun", tanggal_col="tanggal",
        required_cols=["id_konten"],
        header_row=0, data_start_row=1,
    )
