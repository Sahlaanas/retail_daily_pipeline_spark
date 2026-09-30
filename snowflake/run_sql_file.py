"""
run_sql_file.py

Executes every statement in a .sql file against Snowflake, in order.
Built specifically so Airflow can call the numbered scripts under
snowflake/sql/ as pipeline tasks, rather than requiring a human to run
them by hand in Snowsight every time.

Limitation: splits the file naively on blank/comment-only lines around
each ';' — this is fine for the simple, sequential CREATE/COPY INTO
scripts in this project, but would break on a .sql file containing a
semicolon inside a string literal, or a stored-procedure-style
multi-statement block. Don't point this at an arbitrary .sql file
without checking that first.

Usage:
    python snowflake/run_sql_file.py snowflake/sql/05_load_raw.sql
"""
import os
import sys

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "RETAILPULSE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "RETAILPULSE"),
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
    )


def parse_statements(raw: str) -> list[str]:
    statements = []
    for chunk in raw.split(";"):
        code_lines = [
            line for line in chunk.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        if code_lines:
            statements.append("\n".join(code_lines))
    return statements


def run_sql_file(path: str) -> None:
    with open(path, "r") as f:
        raw = f.read()

    statements = parse_statements(raw)
    if not statements:
        print(f"No executable statements found in {path}")
        return

    conn = get_connection()
    cur = conn.cursor()
    try:
        for i, stmt in enumerate(statements, start=1):
            preview = stmt.replace("\n", " ")[:100]
            print(f"[{i}/{len(statements)}] {preview}...")
            cur.execute(stmt)
    finally:
        cur.close()
        conn.close()

    print(f"Done: {len(statements)} statement(s) executed from {path}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python run_sql_file.py <path/to/file.sql>")
    run_sql_file(sys.argv[1])