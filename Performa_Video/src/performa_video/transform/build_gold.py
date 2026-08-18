# src/performa_video/transform/build_gold.py
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

def _load_sql(fname: str) -> str:
    root_dir = Path(__file__).resolve().parents[3]  # etl-performa-video/
    sql_path = root_dir / "sql" / fname
    return sql_path.read_text(encoding="utf-8")


def build_fact_performa_video(bq_client: bigquery.Client) -> pd.DataFrame:
    # 1) ambil performa dari silver_tt_video
    sql_video = _load_sql("select_silver_tt_video.sql")
    df_video = bq_client.query(sql_video).to_dataframe()

    # 2) ambil live_session dari silver_live_session
    sql_production = _load_sql("select_silver_tt_production.sql")
    df_production = bq_client.query(sql_production).to_dataframe()

    # 3) aggregasi performa per id_performa
    num_sum = [
        "views",
        "likes",
        "komentar",
        "dibagikan",
        "pengikut_baru",
        "klik_video_ke_live",
        "produk_dilihat",
        "klik_produk",
        "pembeli",
        "pesanan_video",
        "produk_terjual_video",
        "gmv_bruto_video",
        "gmv_didapat_video",
    ]

    agg_df = df_video.groupby("id_performa").agg(
        {
            **{col: "sum" for col in num_sum},
            "waktu": "first",
            "produk": "first",
        }
    ).reset_index()

    # 4) dedup session dan merge
    merged_df = df_production[
        [
            "id_video",
            "tanggal_production", 
            "scripter", 
            "produk", 
            "editor", 
            "jenis_konten", 
            "tipe_konten", 
            "talent_visual",
            "kategori_konten", 
            "isu", 
            "layout", 
            "script", 
            "cta",
            "talent_vo",
            "referensi_musik",
            "sound",
            "visual_hook",
            "audio_hook", 
            "link_konten"
        ]
    ].merge(
        agg_df[
            [
                "id_video", 
                "tanggal", 
                "toko", 
                "nama_kreator", 
                "informasi_video", 
                "waktu", 
                "produk_sku", 
                "views", 
                "klik_video_ke_live", 
                "produk_dilihat", 
                "klik_produk", 
                "pembeli", 
                "pesanan_video", 
                "produk_terjual_video", 
                "gmv_bruto_video", 
                "gmv_didapat_video"
            ]
        ],
        on="id_video",
        how="right",
    )
    
    return merged_df