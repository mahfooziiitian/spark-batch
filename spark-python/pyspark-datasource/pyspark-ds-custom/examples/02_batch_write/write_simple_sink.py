"""Batch write — write a DataFrame to the `simple_sink` data source.

Key concepts:
    - Implementing DataSourceWriter.write() / commit() / abort()
    - Writing with df.write.format(...).option("path", ...).save()
    - Commit messages aggregated on the driver after all tasks succeed
"""

from __future__ import annotations

import argparse
import os
import tempfile

from custom_ds import SimpleSinkDataSource, create_spark_session

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch write to simple_sink data source")
    parser.add_argument(
        "--output-path",
        default=os.environ.get(
            "OUTPUT_PATH", os.path.join(tempfile.gettempdir(), "custom_ds_sink")
        ),
        help="Output directory for sink files (default: $OUTPUT_PATH or /tmp/custom_ds_sink)",
    )
    args = parser.parse_args()

    spark = create_spark_session("simple-batch-write")

    spark.dataSource.register(SimpleSinkDataSource)

    out_dir: str = args.output_path

    df = spark.range(10).selectExpr("id", "concat('row-', id) as value")
    df.write.format("simple_sink").option("path", out_dir).mode("append").save()

    print(f"wrote rows to: {out_dir}")
    for name in sorted(os.listdir(out_dir)):
        print(f"  {name}")

    spark.stop()
