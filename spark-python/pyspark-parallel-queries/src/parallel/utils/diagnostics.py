"""
Utilities: SparkContext and cluster diagnostics
================================================
Prints a formatted summary of the active SparkSession including version,
master, app name, active executors, configurations, and top Spark UI metrics.
"""

from __future__ import annotations
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def print_session_info(spark: SparkSession) -> None:
    sc = spark.sparkContext
    print("\n── Spark Session ────────────────────────────────")
    print(f"  Version      : {spark.version}")
    print(f"  App name     : {sc.appName}")
    print(f"  Master       : {sc.master}")
    print(f"  App ID       : {sc.applicationId}")
    print(f"  UI port      : {sc.uiWebUrl or 'disabled'}")
    print(f"  Default parallelism : {sc.defaultParallelism}")


def print_active_configs(spark: SparkSession, prefix: str = "spark.sql") -> None:
    conf = spark.sparkContext.getConf().getAll()
    relevant = sorted((k, v) for k, v in conf if k.startswith(prefix))
    print(f"\n── Active configs ({prefix}.*) ────────────────────")
    for k, v in relevant:
        print(f"  {k:<55} = {v}")


def print_executor_info(spark: SparkSession) -> None:
    sc = spark.sparkContext
    status = sc.statusTracker()
    exec_info = status.getExecutorInfos()
    print(f"\n── Executors ({len(exec_info)}) ─────────────────────────────")
    for ex in exec_info:
        print(f"  host={ex.host()}  id={getattr(ex, 'executorId', lambda: '?')()} "
              f"  maxTasks={ex.numRunningTasks()}")


def print_scheduler_mode(spark: SparkSession) -> None:
    mode = spark.conf.get("spark.scheduler.mode", "FIFO")
    xml  = spark.conf.get("spark.scheduler.allocation.file", "(default)")
    print(f"\n── Scheduler ────────────────────────────────────")
    print(f"  Mode   : {mode}")
    print(f"  Config : {xml}")


def print_full_diagnostics(spark: SparkSession) -> None:
    print_session_info(spark)
    print_scheduler_mode(spark)
    print_active_configs(spark, prefix="spark.sql.adaptive")
    print_active_configs(spark, prefix="spark.scheduler")
    print_executor_info(spark)


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))
    from utils.spark_session import get_spark  # noqa: E402 — relative to src/parallel

    spark = get_spark("diagnostics")
    try:
        print_full_diagnostics(spark)
    finally:
        spark.stop()
