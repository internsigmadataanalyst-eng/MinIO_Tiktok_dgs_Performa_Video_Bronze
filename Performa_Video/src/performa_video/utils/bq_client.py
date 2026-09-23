# src/performa_video/utils/bq_client.py
import os

from google.cloud import bigquery
from google.oauth2 import service_account


def get_bq_client() -> bigquery.Client:
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path:
        raise RuntimeError("Env GOOGLE_APPLICATION_CREDENTIALS belum di-set")

    creds = service_account.Credentials.from_service_account_file(sa_path)
    return bigquery.Client(credentials=creds, project=creds.project_id)


def get_credentials():
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path:
        raise RuntimeError("Env GOOGLE_APPLICATION_CREDENTIALS belum di-set")
    return service_account.Credentials.from_service_account_file(sa_path)


def fetch_existing_bronze_hashes(
    creds, table_id, project_id: str,
    min_date: str | None = None,
) -> set:
    """Returns the set of row_hash_raw already present in Bronze,
    scoped to rows at or after min_date (inclusive).

    When min_date is provided, only rows with Tanggal >= min_date are
    scanned — this catches the boundary-day re-emission row that the
    watermark filter always re-selects, while keeping the query cheap.
    """
    from pandas_gbq import read_gbq

    where = ""
    if min_date:
        where = f"WHERE Tanggal >= '{min_date}'"

    df_hashes = read_gbq(
        f"""
        SELECT DISTINCT row_hash_raw
        FROM `{project_id}.{table_id}`
        {where}
        """,
        project_id=project_id,
        credentials=creds,
        dialect="standard",
    )
    return set(df_hashes["row_hash_raw"].dropna().astype(str))