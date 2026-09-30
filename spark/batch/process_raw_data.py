"""
process_raw_data.py

The core PySpark batch job (Part 5). Reads each table's raw Parquet from
S3, cleans/deduplicates/validates it, and writes:
    - valid rows      -> s3a://<bucket>/processed/<table>/
    - rejected rows    -> s3a://<bucket>/quarantine/<table>/  (with a
                          quarantine_reason column explaining why)

Deliberately demonstrates the PySpark techniques called out in the
project doc: window-function dedup, a broadcast join, repartitioning
before write, caching a reused DataFrame, and an explain() plan.

Run with:
    python spark/batch/process_raw_data.py
"""
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from spark.spark_session import get_spark  # noqa: E402

load_dotenv()


def read_raw(spark: SparkSession, bucket: str, table: str) -> DataFrame:
    return spark.read.parquet(f"s3a://{bucket}/raw/{table}/")


def write_processed(df: DataFrame, bucket: str, table: str, num_partitions: int = 4) -> None:
    # repartition() before write: avoids writing hundreds of tiny files
    # (one per input partition/task) when the source data is small, and
    # is the lever you'd tune based on data volume in a real deployment.
    (
        df.repartition(num_partitions)
        .write.mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(f"s3a://{bucket}/processed/{table}/")
    )


def write_quarantine(df: DataFrame, bucket: str, table: str) -> None:
    if df.isEmpty():
        print(f"  quarantine/{table}: nothing to write")
        return
    (
        df.repartition(1)
        .write.mode("append")
        .partitionBy("year", "month", "day")
        .parquet(f"s3a://{bucket}/quarantine/{table}/")
    )


def process_customers(spark: SparkSession, bucket: str) -> None:
    print("\n=== customers ===")
    raw = read_raw(spark, bucket, "customers").cache()  # reused below for both quarantine checks and dedup
    print(f"  raw rows: {raw.count()}")

    # ── Validity checks: NULL email, future signup_date ─────────────────
    invalid = raw.filter(
        F.col("email").isNull() | (F.col("signup_date") > F.current_date())
    ).withColumn(
        "quarantine_reason",
        F.when(F.col("email").isNull(), F.lit("null_email"))
         .otherwise(F.lit("future_signup_date")),
    )
    write_quarantine(invalid, bucket, "customers")
    print(f"  quarantined (null email / future signup_date): {invalid.count()}")

    valid = raw.filter(F.col("email").isNotNull() & (F.col("signup_date") <= F.current_date()))

    # ── Dedup: same person (name, email, city, state, signup_date)
    # inserted under multiple customer_id values. Keep the earliest.
    dedup_key = ["name", "email", "city", "state", "signup_date"]
    window = Window.partitionBy(*dedup_key).orderBy(F.col("customer_id").asc())
    deduped = (
        valid.withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    dropped_dupes = valid.count() - deduped.count()
    print(f"  duplicate customers dropped: {dropped_dupes}")

    write_processed(deduped, bucket, "customers")
    print(f"  processed rows written: {deduped.count()}")
    raw.unpersist()


def process_products(spark: SparkSession, bucket: str) -> DataFrame:
    print("\n=== products ===")
    raw = read_raw(spark, bucket, "products")
    print(f"  raw rows: {raw.count()}")

    invalid = raw.filter(F.col("price") < 0).withColumn(
        "quarantine_reason", F.lit("negative_price")
    )
    write_quarantine(invalid, bucket, "products")
    print(f"  quarantined (negative price): {invalid.count()}")

    valid = raw.filter(F.col("price") >= 0)
    write_processed(valid, bucket, "products")
    print(f"  processed rows written: {valid.count()}")

    # Returned (not just written) because order_items processing below
    # needs it for a broadcast join.
    return valid


def process_orders(spark: SparkSession, bucket: str) -> DataFrame:
    print("\n=== orders ===")
    raw = read_raw(spark, bucket, "orders")
    print(f"  raw rows: {raw.count()}")

    # Dedup: same order content (customer, store, order_date, total_amount)
    # inserted under multiple order_id values — simulates an
    # at-least-once delivery bug upstream. Keep the earliest order_id.
    dedup_key = ["customer_id", "store_id", "order_date", "total_amount"]
    window = Window.partitionBy(*dedup_key).orderBy(F.col("order_id").asc())
    deduped = (
        raw.withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    dropped_dupes = raw.count() - deduped.count()
    print(f"  duplicate orders dropped: {dropped_dupes}")

    write_processed(deduped, bucket, "orders")
    print(f"  processed rows written: {deduped.count()}")
    return deduped


def process_order_items(spark: SparkSession, bucket: str) -> None:
    print("\n=== order_items ===")
    raw = read_raw(spark, bucket, "order_items")
    print(f"  raw rows: {raw.count()}")

    # Referential integrity here is checked against EVERY known product_id
    # — including ones quarantined elsewhere for a negative price. Price
    # validity and referential existence are independent quality
    # dimensions: a product with a bad price is still a real product, so
    # an order_item pointing at it is a perfectly valid reference and
    # must not be conflated with the "points at a product_id that was
    # never real at all" case this join is actually meant to catch.
    all_product_ids = read_raw(spark, bucket, "products").select("product_id").distinct()

    # Broadcast join: all_product_ids is small (hundreds of rows) compared
    # to order_items (thousands+), so broadcasting it avoids a shuffle of
    # the larger side entirely.
    #
    # left_semi/left_anti are used (rather than a left_outer + null-check,
    # which was tried here originally) specifically because they can't be
    # optimized away: a left_outer join against a DataFrame whose only
    # column is the join key itself adds no new information and changes
    # no row count, so Catalyst legitimately eliminates it entirely — a
    # semi/anti join, by contrast, is a genuine existence filter that has
    # to actually execute.
    invalid = raw.join(
        F.broadcast(all_product_ids), on="product_id", how="left_anti"
    ).withColumn("quarantine_reason", F.lit("invalid_product_id"))
    write_quarantine(invalid, bucket, "order_items")
    print(f"  quarantined (invalid product_id): {invalid.count()}")

    valid = raw.join(F.broadcast(all_product_ids), on="product_id", how="left_semi")

    # This is the interview-relevant bit: printing the physical plan for
    # the join that's actually used shows a BroadcastHashJoin rather than
    # a SortMergeJoin/shuffle.
    print("  physical plan for the order_items <-> products join:")
    valid.explain()

    write_processed(valid, bucket, "order_items")
    print(f"  processed rows written: {valid.count()}")


def main():
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        raise SystemExit("S3_BUCKET is not set in .env")

    spark = get_spark("retailpulse-batch-transform")
    spark.sparkContext.setLogLevel("WARN")

    process_customers(spark, bucket)
    process_products(spark, bucket)
    process_orders(spark, bucket)
    process_order_items(spark, bucket)

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()