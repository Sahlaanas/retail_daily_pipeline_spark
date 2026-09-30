"""
extract_to_s3.py

Incrementally extracts every OLTP table into the S3 raw data lake as
partitioned Parquet, using the pipeline_watermark table so each run only
pulls rows changed since the last successful run.

S3 layout produced:
    s3://<bucket>/raw/<table>/year=YYYY/month=MM/day=DD/<table>_<run_ts>.parquet

Run with:
    python ingestion/s3/extract_to_s3.py
"""
import os
import tempfile
from datetime import datetime, timezone

import boto3
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

TABLES = [
    "suppliers", "stores", "products", "customers",
    "orders", "order_items", "payments", "inventory",
]

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def get_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}"
        f"@{os.getenv('PGHOST', 'localhost')}:{os.getenv('PGPORT', '5432')}"
        f"/{os.getenv('PGDATABASE')}"
    )
    return create_engine(url)


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


def get_watermark(engine, pipeline_name: str) -> datetime:
    """Returns the last_processed_timestamp for this pipeline, or the
    epoch if it has never run before (first full extract)."""
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT last_processed_timestamp FROM pipeline_watermark "
                "WHERE pipeline_name = :name"
            ),
            {"name": pipeline_name},
        ).fetchone()
    if row is None or row[0] is None:
        return EPOCH
    return row[0]


def update_watermark(engine, pipeline_name: str, new_timestamp):
    """Upserts the watermark row. last_successful_run always advances to
    now() (the run genuinely succeeded); last_processed_timestamp only
    advances if new_timestamp was actually provided (i.e. some rows were
    found) — if there was nothing new, there's nothing to move past."""
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO pipeline_watermark
                    (pipeline_name, last_processed_timestamp, last_successful_run)
                VALUES (:name, :ts, now())
                ON CONFLICT (pipeline_name) DO UPDATE SET
                    last_processed_timestamp = COALESCE(:ts, pipeline_watermark.last_processed_timestamp),
                    last_successful_run = now()
                """
            ),
            {"name": pipeline_name, "ts": new_timestamp},
        )


def extract_table(engine, s3, bucket: str, table: str, run_ts: datetime) -> int:
    pipeline_name = f"extract_{table}"
    watermark = get_watermark(engine, pipeline_name)

    query = text(f"SELECT * FROM {table} WHERE updated_at > :watermark ORDER BY updated_at")
    df = pd.read_sql(query, engine, params={"watermark": watermark})

    if df.empty:
        print(f"  {table}: no new/changed rows since {watermark}")
        update_watermark(engine, pipeline_name, None)
        return 0

    new_watermark = df["updated_at"].max()

    local_path = os.path.join(
        tempfile.gettempdir(), f"{table}_{run_ts:%Y%m%d%H%M%S}.parquet"
    )
    df.to_parquet(local_path, index=False)

    key = (
        f"raw/{table}/year={run_ts:%Y}/month={run_ts:%m}/day={run_ts:%d}/"
        f"{table}_{run_ts:%Y%m%d%H%M%S}.parquet"
    )
    s3.upload_file(local_path, bucket, key)
    os.remove(local_path)

    update_watermark(engine, pipeline_name, new_watermark)
    print(f"  {table}: {len(df)} rows -> s3://{bucket}/{key}")
    return len(df)


def main():
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        raise SystemExit("S3_BUCKET is not set in .env")

    engine = get_engine()
    s3 = get_s3_client()
    run_ts = datetime.now(timezone.utc)

    print(f"Starting extract run at {run_ts.isoformat()}")
    total = 0
    for table in TABLES:
        total += extract_table(engine, s3, bucket, table, run_ts)

    print(f"\nDone. {total} total rows extracted across {len(TABLES)} tables.")


if __name__ == "__main__":
    main()