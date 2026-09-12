"""Run every statement in a repository ``.sql`` file and print the results.

Reuses the same comment-aware splitter the pytest suite uses, so the file runs
exactly as it is tested.

    uv run python examples/run_sql_file.py
    uv run python examples/run_sql_file.py sql/window/ranking/ranking.sql
"""

from __future__ import annotations

import os
import sys

from _session import local_session

from spark_sql.runner import SqlRunner

DEFAULT_SQL_FILE = "sql/nulls/nulls.sql"

if os.environ.get("JAVA_HOME") is None:
    os.environ["JAVA_HOME"] =   os.environ.get("JAVA_HOME_17","/home/malam/.sdkman/candidates/java/17.0.18-amzn")

def main(sql_file: str) -> None:
    spark = local_session("run-sql-file")
    try:
        runner = SqlRunner.local(spark)
        results = runner.run_file(sql_file, queries_only=True)

        print(f"Ran {sql_file}: {len(results)} query result(s)\n")
        for index, result in enumerate(results, start=1):
            print(f"--- query {index}: {result.column_count} col(s), {result.row_count} row(s) ---")
            print("columns:", result.columns)
            for row in result.rows[:5]:
                print("  ", row)
            if result.row_count > 5:
                print(f"   ... ({result.row_count - 5} more)")
            print()
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SQL_FILE)
