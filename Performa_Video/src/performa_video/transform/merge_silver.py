# src/performa_video/transform/merge_silver.py
from pathlib import Path
from performa_video.utils.bq_client import get_bq_client


def merge_to_silver():
    bq_client = get_bq_client()

    root_dir = Path(__file__).resolve().parents[3]  # etl-performa-video/
    sql_path = root_dir / "sql" / "silver_merge_tt_produk.sql"

    merge_sql = sql_path.read_text(encoding="utf-8")
    job = bq_client.query(merge_sql)
    job.result()
    print("Silver MERGE OK.")