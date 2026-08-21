# src/performa_video/transform/merge_silver.py
from pathlib import Path
from src.performa_video.utils.bq_client import get_bq_client


def create_gold_fact_video_performa_daily():
    bq_client = get_bq_client()

    root_dir = Path(__file__).resolve().parents[3]  # etl-performa-video/
    sql_path = root_dir / "sql" / "gold_create_or_replace_fact_video_performa_daily.sql"

    merge_sql = sql_path.read_text(encoding="utf-8")
    job = bq_client.query(merge_sql)
    job.result()
    print("GOLD CREATE OK.")