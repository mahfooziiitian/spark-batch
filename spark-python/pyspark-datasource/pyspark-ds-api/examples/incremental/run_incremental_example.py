"""End-to-end example: run incremental ingestion twice in a row and show that
the second run only pulls records newer than the first run's watermark.

Prerequisites:
    PYTHONPATH=src uv run python examples/incremental/mock_incremental_server.py

Run:
    PYTHONPATH=src uv run python examples/incremental/run_incremental_example.py
"""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from rest_ds.incremental.incremental_runner import run_incremental_ingestion
from rest_ds.incremental.state_store import IncrementalStateStore
from rest_ds.util.config_loader import load_config

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
os.environ["PYSPARK_PYTHON"] = sys.executable

SOURCE_NAME = "events_api"


def main():
    spark = (
        SparkSession.builder.appName("Incremental_REST_API_Ingestion")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    config_path = Path(__file__).parent / "incremental_api_source.yaml"
    config = load_config(config_path)

    # Use a project-local sqlite file for the control table in this example
    # (the same URL declared in incremental_api_source.yaml's `stateStore`).
    state_store = IncrementalStateStore(
        config["extracts"]["extract"]["source"]["params"]["options"]["incremental"][
            "stateStore"
        ]["url"]
    )

    print("\n=== Run 1 (first run — uses initialValue as the starting watermark) ===")
    df1 = run_incremental_ingestion(spark, config, SOURCE_NAME, state_store=state_store)
    print(f"Run 1 fetched {df1.count()} records")
    df1.show(5, truncate=False)

    print(
        "\n=== Run 2 (immediately after — only the lookback overlap + any records missed by run 1's limit) ==="
    )
    df2 = run_incremental_ingestion(spark, config, SOURCE_NAME, state_store=state_store)
    print(f"Run 2 fetched {df2.count()} records")

    print("\n=== Run history for reconciliation ===")
    for run in state_store.get_history(SOURCE_NAME):
        print(
            f"run_id={run.run_id} status={run.status} "
            f"watermark={run.watermark_start!r} -> {run.watermark_end!r} "
            f"records={run.records_fetched}"
        )

    spark.stop()


if __name__ == "__main__":
    main()
