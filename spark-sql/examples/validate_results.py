"""Validate results of a real ``sql/`` example with the fluent ``expect`` API.

Runs ``sql/window/ranking/ranking.sql`` and asserts business expectations about
the ranking output.

    uv run python examples/validate_results.py
"""

from __future__ import annotations

import os

from _session import local_session

from spark_sql._helpers import statement_containing
from spark_sql.runner import SqlRunner, ValidationError, expect

SQL_FILE = "sql/window/ranking/ranking.sql"

if os.environ.get("JAVA_HOME") is None:
    # IDE run configs (e.g. PyCharm) don't always inherit shell env vars, so fall
    # back to JAVA_HOME_17 (or a local sdkman install) when JAVA_HOME is unset.
    os.environ["JAVA_HOME"] = os.environ.get(
        "JAVA_HOME_17", "/home/malam/.sdkman/candidates/java/17.0.18-amzn"
    )

def main() -> None:
    spark = local_session("validate-results")
    try:
        runner = SqlRunner.local(spark)

        # Execute the whole file so its TEMP VIEWs exist, then re-run one query by content.
        runner.run_file(SQL_FILE)
        ranking_stmt = statement_containing(SQL_FILE, "ROW_NUMBER")
        result = runner.run(ranking_stmt)

        print(f"ranking query -> {result.row_count} row(s), columns {result.columns}")
        expect(result).non_empty().min_rows(9)
        print("expectation passed: non-empty with >= 9 rows")

        # Demonstrate a failing expectation being caught.
        try:
            expect(result).row_count(1)
        except ValidationError as exc:
            print("caught expected ValidationError:", exc)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
