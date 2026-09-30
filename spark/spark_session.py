"""
spark_session.py

Shared SparkSession builder. Every Spark script in this project (batch,
streaming, and anything added later) should get its session from here,
so the S3A / credentials configuration lives in exactly one place.

Uses local jars from spark/jars/ (hadoop-aws + aws-java-sdk-bundle, and
whatever else you've placed there) rather than spark.jars.packages, which
tries to resolve dependencies from Maven Central over the network via Ivy
— often slow or flaky on Windows. Point JARS_DIR at wherever you put them
if it's not the default location.
"""
import os

from pyspark.sql import SparkSession

# Directory containing your downloaded jars (hadoop-aws-*.jar,
# aws-java-sdk-bundle-*.jar, etc). Defaults to spark/jars relative to
# this file; override with the JARS_DIR env var if yours lives elsewhere.
JARS_DIR = os.getenv(
    "JARS_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "jars"),
)


def get_spark(app_name: str) -> SparkSession:
    # Java classpath wildcard: picks up every .jar in JARS_DIR without
    # having to list them by name individually.
    jars_classpath = os.path.join(JARS_DIR, "*")

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.extraClassPath", jars_classpath)
        .config("spark.executor.extraClassPath", jars_classpath)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"s3.{os.getenv('AWS_DEFAULT_REGION', 'us-east-1')}.amazonaws.com",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                 "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Don't write the empty _SUCCESS marker file Hadoop's
        # FileOutputCommitter drops in every output folder by default —
        # downstream readers that glob/wildcard the folder (e.g.
        # Snowflake's COPY INTO) will otherwise try to parse it as Parquet
        # too and fail on it being a 0-byte file.
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .getOrCreate()
    )