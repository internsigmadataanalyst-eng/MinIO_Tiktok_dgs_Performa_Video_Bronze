# src/performa_video/transform/clean_bronze.py
import uuid
import hashlib
from datetime import datetime, timezone

import pandas as pd

from src.performa_video.utils.transform_utils import (
    parse_mixed_dates,
    to_snake_case,
)
from src.performa_video.utils.minio_client import filter_by_sheet_watermark

# Daftar kolom yang ingin dibuang dari produksi (dalam format snake_case)
DROP_COLS_PRODUKSI = [
    "kode",
    "no_video",
    "no_cep",
    "funnel",
    "struktur",
    "cvp",
    "platform",
    "caption",
    "link_drive",
    "bulan",
]

def _canon(x):
    import pandas as pd

    x = "" if pd.isna(x) else str(x).strip()
    return x.upper()


def build_bronze_video(
    tiktok_video_raw: pd.DataFrame, sheet_watermarks: dict | None = None
) -> tuple[pd.DataFrame, dict]:
    """
    Dari raw GSheet → cleaning numeric + tanggal + snake_case,
    tambah snapshot_ts, snapshot_date, run_id, row_hash_raw.
    Filter incremental per sheet_name berdasarkan watermark (sheet_watermarks).
    Output: (df siap di-load ke BRONZE_DB.bronze_live, sheet_max_dates)
    """
    # numeric cleaning sudah dilakukan di STEP 2 (validate_and_normalize_raw).
    tiktok_video_clean1 = tiktok_video_raw.copy()

    # parse tanggal
    tiktok_video_clean1["Tanggal"] = parse_mixed_dates(
        tiktok_video_clean1["Tanggal"], return_date=False
    )
    tiktok_video_clean1["Waktu"] = parse_mixed_dates(
        tiktok_video_clean1["Waktu"], return_date=False
    )

    # copy & snake_case
    df = tiktok_video_clean1.copy()
    df.columns = df.columns.map(to_snake_case)

    # buang baris tanpa id
    df = df[df["tanggal"].astype(str).str.strip() != ""]

    # Drop kolom-kolom yang tidak diperlukan sebelum dikirim ke Minio
    df = df.drop(columns=DROP_COLS_PRODUKSI, errors="ignore")

    # Replace empty strings dengan None di kolom berjenis object/string
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].replace("", None)

    # snapshot fields
    now_utc = datetime.now(timezone.utc)
    df["snapshot_ts"] = now_utc
    df["snapshot_date"] = now_utc.date()
    df["run_id"] = str(uuid.uuid4())

    # row_hash_raw: sesuai scriptmu
    cols_for_hash = ["tanggal","toko","nama_kreator","id_video","vv","gmv_yang_didapat_dari_video_jualan_rp"]

    df["row_hash_raw"] = (
        df[cols_for_hash]
        .map(_canon)
        .astype(str)
        .agg("||".join, axis=1)
        .apply(lambda s: hashlib.sha256(s.encode()).hexdigest())
    )

    columns_to_int_bq = [
        'vv',
        'likes',
        'komentar',
        'dibagikan',
        'pengikut_baru',
        'klik_video_ke_live',
        'produk_dilihat',
        'klik_produk',
        'pembeli',
        'pesanan_video',
        'produk_yang_terjual_dari_video',
    ]

    for col in columns_to_int_bq:
        if col in df.columns:
            df[col] = (
                pd.to_numeric(df[col], errors='coerce')
                .fillna(0)
                .apply(lambda x: int(round(x)))
                .astype(pd.Int64Dtype())
            )

    # Filter incremental per sheet (creds-keyed) berdasarkan watermark
    if "creds" in df.columns:
        df, sheet_max_dates = filter_by_sheet_watermark(
            df, "creds", "tanggal", sheet_watermarks or {}
        )
    else:
        sheet_max_dates = {}

    # NOTE: creds & sheet_name sengaja DIPERTAHANKAN di level bronze.
    return df, sheet_max_dates

def build_bronze_produksi(
    tiktok_produksi_raw: pd.DataFrame, sheet_watermarks: dict | None = None
) -> tuple[pd.DataFrame, dict]:
    """
    Dari raw GSheet → cleaning numeric + tanggal + snake_case,
    tambah snapshot_ts, snapshot_date, run_id, row_hash_raw.
    Filter incremental per sheet_name berdasarkan watermark (sheet_watermarks).
    Output: (df siap di-load ke MinIO, sheet_max_dates)
    """

    # copy & snake_case
    tiktok_produksi_clean1 = tiktok_produksi_raw.copy()
    tiktok_produksi_clean1.columns = tiktok_produksi_clean1.columns.map(to_snake_case)

    # Remove duplicate columns, keeping the first occurrence
    tiktok_produksi_clean1 = tiktok_produksi_clean1.loc[:, ~tiktok_produksi_clean1.columns.duplicated()]

    # buang baris tanpa id
    df = tiktok_produksi_clean1.copy()
    df = df[df["id_konten"].astype(str).str.strip() != ""]

    # parse tanggal
    df["tanggal"] = parse_mixed_dates(
        df["tanggal"], return_date=False
    )
    df["tanggal_jadi"] = parse_mixed_dates(
        df["tanggal_jadi"], return_date=False
    )

    # snapshot fields
    now_utc = datetime.now(timezone.utc)
    df["snapshot_ts"] = now_utc
    df["snapshot_date"] = now_utc.date()
    df["run_id"] = str(uuid.uuid4())

    # row_hash_raw: sesuai scriptmu
    cols_for_hash = ["tanggal","scripter","id_konten"]

    df["row_hash_raw"] = (
        df[cols_for_hash]
        .map(_canon)
        .astype(str)
        .agg("||".join, axis=1)
        .apply(lambda s: hashlib.sha256(s.encode()).hexdigest())
    )

    # Filter incremental per sheet (creds-keyed) berdasarkan watermark
    if "creds" in df.columns:
        df, sheet_max_dates = filter_by_sheet_watermark(
            df, "creds", "tanggal", sheet_watermarks or {}
        )
    else:
        sheet_max_dates = {}

    # NOTE: creds & sheet_name sengaja DIPERTAHANKAN di level bronze.
    return df, sheet_max_dates