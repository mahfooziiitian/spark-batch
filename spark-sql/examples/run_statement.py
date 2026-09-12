"""Run a single SQL statement on local Spark and validate the result.

    uv run python examples/run_statement.py
"""

from __future__ import annotations

import os

from _session import local_session

from spark_sql.runner import SqlRunner, expect, summarize

if os.environ.get("JAVA_HOME") is None:
    os.environ["JAVA_HOME"] =   os.environ.get("JAVA_HOME_17","/home/malam/.sdkman/candidates/java/17.0.18-amzn")

def main() -> None:
    spark = local_session("run-statement")
    try:
        runner = SqlRunner.local(spark)

        result = runner.run("SELECT 1 AS n, 'spark-sql' AS engine")
        print("columns:", result.columns)
        print("rows:   ", result.rows)
        print("summary:", summarize(result))

        # Assertions raise ValidationError on failure.
        expect(result).non_empty().row_count(1).columns(["n", "engine"])
        print("validation: OK")

        # Capture the physical plan alongside the result.
        explained = runner.run("SELECT 1 AS n", explain=True)
        print("\n== physical plan ==")
        print(explained.plan)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
