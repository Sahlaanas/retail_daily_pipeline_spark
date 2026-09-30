"""
retail_daily_pipeline.py

Orchestrates the full RetailPulse pipeline: incremental Postgres
extract -> S3 raw zone -> PySpark clean/dedup -> S3 processed zone ->
Snowflake RAW + BRONZE loads -> dbt snapshot/run/test.

Dependency graph (not a single linear chain — two branches genuinely
run in parallel):

    extract_postgres ─┬─> spark_transform ──> load_snowflake_bronze ─┐
                       └─> load_snowflake_raw ─────────────────────┬─┴─> dbt_snapshot -> dbt_run -> dbt_tests

    (load_snowflake_raw only needs the S3 raw/ zone, which
    extract_postgres alone produces — it doesn't need to wait on Spark.
    load_snowflake_bronze needs Spark's processed/ zone, so it does.)
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"
VENV_BIN = "/home/airflow/venvs/retailpulse/bin"

default_args = {
    "owner": "retailpulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="retail_daily_pipeline",
    description="RetailPulse: Postgres -> S3 -> Spark -> Snowflake -> dbt",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["retailpulse"],
) as dag:

    extract_postgres = BashOperator(
        task_id="extract_postgres",
        bash_command=f"cd {PROJECT_DIR} && {VENV_BIN}/python ingestion/s3/extract_to_s3.py",
    )

    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=f"cd {PROJECT_DIR} && {VENV_BIN}/python spark/batch/process_raw_data.py",
    )

    load_snowflake_raw = BashOperator(
        task_id="load_snowflake_raw",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{VENV_BIN}/python snowflake/run_sql_file.py snowflake/sql/05_load_raw.sql"
        ),
    )

    load_snowflake_bronze = BashOperator(
        task_id="load_snowflake_bronze",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{VENV_BIN}/python snowflake/run_sql_file.py snowflake/sql/06_load_bronze.sql"
        ),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {PROJECT_DIR}/dbt/retail_dbt && {VENV_BIN}/dbt --no-partial-parse snapshot",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {PROJECT_DIR}/dbt/retail_dbt && {VENV_BIN}/dbt --no-partial-parse run",
    )

    dbt_tests = BashOperator(
        task_id="dbt_tests",
        bash_command=f"cd {PROJECT_DIR}/dbt/retail_dbt && {VENV_BIN}/dbt --no-partial-parse test",
    )

    extract_postgres >> spark_transform >> load_snowflake_bronze
    extract_postgres >> load_snowflake_raw
    [load_snowflake_raw, load_snowflake_bronze] >> dbt_snapshot >> dbt_run >> dbt_tests