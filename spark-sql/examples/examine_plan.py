"""Examine how a join executes: capture the plan and assert on its operators.

    uv run python examples/examine_plan.py
"""

from __future__ import annotations

import os

from _session import local_session

from spark_sql.runner import SqlExaminer, SqlRunner, expect_plan, summarize

JOIN_SQL = """
SELECT o.order_id, c.name, o.amount
FROM (VALUES (1, 101, 250.0), (2, 102, 80.0), (3, 101, 430.0)) AS o(order_id, customer_id, amount)
JOIN (VALUES (101, 'Alice'), (102, 'Bob')) AS c(customer_id, name)
ON o.customer_id = c.customer_id
"""

GROUP_SQL = """
SELECT customer_id, count(*) AS orders, sum(amount) AS total
FROM (VALUES (1, 101, 250.0), (2, 102, 80.0), (3, 101, 430.0)) AS o(order_id, customer_id, amount)
GROUP BY customer_id
"""

if os.environ.get("JAVA_HOME") is None:
    os.environ["JAVA_HOME"] =   os.environ.get("JAVA_HOME_17","/home/malam/.sdkman/candidates/java/17.0.18-amzn")

def main() -> None:
    spark = local_session("examine-plan")
    try:
        runner = SqlRunner.local(spark)
        examiner = SqlExaminer(runner)

        join_plan = examiner.plan_summary(JOIN_SQL)
        print("== join plan ==")
        print("exchanges:      ", join_plan.exchanges)
        print("join strategies:", join_plan.join_strategies)
        print("scans:          ", join_plan.scans)
        # A tiny dimension is broadcast — no shuffle for the join.
        expect_plan(join_plan).broadcast_join().no_cartesian()
        print("join assertions: OK\n")

        group_plan = examiner.plan_summary(GROUP_SQL)
        print("== group-by plan ==")
        print("has shuffle:", group_plan.has_shuffle, "| exchanges:", group_plan.exchanges)

        result = runner.run(GROUP_SQL)
        print("result summary:", summarize(result))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
