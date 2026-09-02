"""Databricks job task: distributed Excel ingestion via the spark-excel connector.

Consumes a *different* library than ``ingest_excel_to_delta.py``: instead of the
pandas bridge, this task drives ``pys_excel.spark_excel`` against the community
`spark-excel <https://github.com/crealytics/spark-excel>`_ Maven library
(``com.crealytics:spark-excel_2.12:3.5.1_0.20.4``), attached to the job cluster
via ``libraries.maven`` in resources/excel_ingestion_job.job.yml. Reads are
distributed across executors rather than collected to the driver, so this path
is preferred for cluster-scale workbooks.

On Databricks Runtime 17.1+, ``resolve_excel_format()`` automatically switches
to the built-in ``excel`` format instead, with no code change required.

Run via: databricks bundle run excel_ingestion_job -t <target>
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from pys_excel.spark_excel import read_spark_excel, resolve_excel_format


def main() -> None:
    parser = argparse.ArgumentParser(description="Distributed Excel ingestion via spark-excel.")
    parser.add_argument("--input-path", required=True, help="Volumes path to the source .xlsx workbook.")
    parser.add_argument("--table-name", required=True, help="Destination catalog.schema.table.")
    parser.add_argument("--data-address", default="'Sheet1'!A1", help="Sheet/cell range, e.g. \"'Employees'!A1\".")
    parser.add_argument("--mode", default="overwrite", help="Save mode: overwrite, append, ignore, error.")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    fmt = resolve_excel_format(spark)
    print(f"Resolved Excel format for this runtime: {fmt}")

    df = read_spark_excel(spark, args.input_path, data_address=args.data_address)
    df.write.format("delta").mode(args.mode).saveAsTable(args.table_name)

    print(f"Loaded {df.count()} rows from {args.input_path} into {args.table_name} via {fmt}")


if __name__ == "__main__":
    main()
