# src/performa_video/utils/bronze_compare.py
"""Show the current MinIO watermark.

Ported from Pesanan_Affiliasi and parameterized per project so it works for the
sibling ETLs. Delivered as a copied module per repository.

Highlights:
- `effective_watermark_changes` : only reports watermark updates whose candidate
  max-date actually differs from the stored value (avoids fake 'watermark update'
  lines in dry-run when nothing changes).
- `show_watermark` : shows only the MinIO watermark (no bronze read/compare).
"""
import pandas as pd

PROJECT_ID = "database-sigma"


def effective_watermark_changes(
    watermark_records: list, sheet_max_dates: dict
) -> dict:
    """Returns only the (creds, sheet_name, toko) -> max_date entries whose
    candidate max_date actually DIFFERS from the currently-stored watermark.

    Groups with no stored watermark yet (new groups) are included. This avoids
    reporting a 'watermark update' when nothing would change.
    """
    stored = {}
    for rec in watermark_records or []:
        creds = str(rec.get("creds") or "")
        sheet_name = str(rec.get("sheet_name") or "")
        toko = str(rec.get("toko") or "")
        date_val = str(rec.get("last_processed_date") or rec.get("last_update") or "").strip()[:10]
        stored[(creds, sheet_name, toko)] = date_val

    changes = {}
    for (creds, sheet_name, toko), max_date in sheet_max_dates.items():
        key = (str(creds), str(sheet_name or ""), str(toko or ""))
        if max(stored.get(key, ""), "").strip() == str(max_date).strip()[:10]:
            continue
        changes[key] = str(max_date)
    return changes


def _show_watermark(watermark_records: list):
    if not watermark_records:
        print("[WATERMARK] (tidak ada watermark MinIO yang ditemukan)")
        return
    print("\n[WATERMARK] Current watermark (MinIO):")
    wm_cols = [c for c in ("creds", "sheet_name", "toko", "last_processed_date")
               if c in (watermark_records[0] or {})]
    wm_df = pd.DataFrame(watermark_records)[wm_cols].sort_values(wm_cols)
    print(wm_df.to_string(index=False))


def show_watermark(watermark_records: list):
    """Shows only the MinIO watermark (no bronze read/compare)."""
    _show_watermark(watermark_records)