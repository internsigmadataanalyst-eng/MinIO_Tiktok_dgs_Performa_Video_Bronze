# src/performa_video/utils/recovery.py
"""Error-recovery path-A logic: select rows from resolved / changed errors."""

import pandas as pd


def select_recovered(
    df_valid: pd.DataFrame,
    resolved: list,
    report: dict,
    date_col: str = "Tanggal",
    grain_col: str = "toko",
    readmit: list | None = None,
) -> pd.DataFrame:
    """Select rows from *df_valid* that belong to a recovered error group.

    Grain is (sheet_name, creds, grain, error_date) — grain verbatim ('' for
    produksi, which has no blank-aware grain column). Two kinds of group are
    passed in, and both are treated identically: select EVERY valid row carrying
    the group's key.

      - `resolved`: the group is no longer detected as bad this run (defect gone,
        or a date_future / legacy future-date entry remediated).
      - `readmit` : the group is still detected, but its signature
        (n_rows, error_reasons shape) changed — some rows were fixed, or the
        defect changed shape.

    Re-admitting a whole group is deliberately NOT count-matched. A strict
    `count == n_rows` test could never fire for a partially fixed date: fixing
    some rows lowers the count while the group stays open, so the entry
    deadlocks forever and the already-fixed rows are dropped by the watermark
    filter on every subsequent run, because the sheet date has not moved.
    Here the whole group is re-admitted and the downstream layers sort it out:

    - `row_hash_raw` covers `tanggal`, the grain, and (video) the numeric
      columns `vv` / `gmv_yang_didapat_dari_video_jualan_rp`. A value-corrected
      row therefore gets a fresh hash and survives the bronze idempotency gate
      as a new revision, while an unchanged row collides with its bronze twin
      and is dropped.
    - The silver MERGE de-duplicates bronze by business key
      (ROW_NUMBER ... ORDER BY snapshot_ts DESC, run_id DESC) and only UPDATEs
      when `row_hash_clean` differs — so the correction lands without
      double-counting.

    Callers must pass these rows through build_bronze_* with an EMPTY watermark
    map so the watermark date cannot gate them; the returned frame bypasses the
    regular incremental path. Both builders use `sheet_watermarks or {}`, so an
    empty dict is a no-op pass-through rather than a full load.

    No honesty filter is applied here, unlike Data_Live / GMV_Max_Produk: those
    mirror a hard drop inside build_bronze_* that could remove a re-admitted
    row. In this project both drop conditions are already enforced upstream —
    rows with a blank `tanggal` (video) or blank `id_konten` (produksi) are
    removed before validation, and blank-grain rows are routed to df_error by
    validate_and_normalize_raw — so df_valid can never carry a row that
    build_bronze_* would discard.

    Counters added to ``report``:
      recovery_resolved        : fully resolved groups considered
      recovery_partial         : still-broken groups whose signature changed
      recovery_readmit_rows    : rows selected for Path A
      recovery_absent          : groups with no matching valid rows (deleted)
    """
    df = df_valid.copy()

    candidates = list(resolved or []) + list(readmit or [])

    if df.empty or not candidates:
        report.setdefault("recovery_resolved", 0)
        report.setdefault("recovery_partial", 0)
        report.setdefault("recovery_readmit_rows", 0)
        report.setdefault("recovery_absent", 0)
        return df.iloc[0:0]

    raw_grain = next(
        (c for c in df.columns if str(c).strip().lower() == grain_col.lower()), None
    )
    if raw_grain is not None:
        grain_series = df[raw_grain].astype(str)
    else:
        grain_series = pd.Series("", index=df.index, dtype=str)

    try:
        tanggal_str = pd.to_datetime(df[date_col]).dt.date.astype(str)
    except Exception:
        tanggal_str = df[date_col].astype(str)

    key_series = (
        df["sheet_name"].astype(str)
        + "|" + df["creds"].astype(str)
        + "|" + grain_series
        + "|" + tanggal_str
    )

    match = pd.Series(False, index=df.index)
    absent = 0

    for r in candidates:
        key = f'{r["sheet_name"]}|{r["creds"]}|{r.get("toko") or ""}|{r["error_date"]}'
        grp = df.index[key_series == key]
        if len(grp) == 0:
            absent += 1
        else:
            match.loc[grp] = True

    report["recovery_resolved"] = len(resolved or [])
    report["recovery_partial"] = len(readmit or [])
    report["recovery_readmit_rows"] = int(match.sum())
    report["recovery_absent"] = absent

    return df[match]
