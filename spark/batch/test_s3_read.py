"""
test_s3_read.py

Sanity check before writing the real batch transformation job: creates a
SparkSession wired up for S3A access (using the same AWS credentials as
the rest of the project's .env), reads the raw customers Parquet files,
and prints the schema + row count.

If this runs cleanly, your Java + winutils + Spark + S3 connector setup
is all working and we can move on to the actual PySpark transform job.

Run with:
    python spark/batch/test_s3_read.py
"""
import os
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from spark.spark_session import get_spark  # noqa: E402

load_dotenv()


def main():
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        raise SystemExit("S3_BUCKET is not set in .env")

    spark = get_spark("retailpulse-s3-test")
    spark.sparkContext.setLogLevel("WARN")

    path = f"s3a://{bucket}/raw/customers/"
    print(f"Reading {path} ...")
    df = spark.read.parquet(path)

    print("\nSchema:")
    df.printSchema()

    print(f"\nRow count: {df.count()}")
    df.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()