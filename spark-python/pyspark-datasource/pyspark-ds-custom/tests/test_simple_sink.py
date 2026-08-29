from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from custom_ds import SimpleSinkDataSource

pytestmark = pytest.mark.pyspark


def test_simple_sink_writes_all_rows(spark: SparkSession, tmp_path) -> None:
    spark.dataSource.register(SimpleSinkDataSource)

    df = spark.range(10).selectExpr("id", "concat('row-', id) as value")
    df.write.format("simple_sink").option("path", str(tmp_path)).mode("append").save()

    written_files = list(tmp_path.glob("part-*.jsonl"))
    assert written_files, "expected at least one partition file to be written"

    total_lines = sum(len(f.read_text().splitlines()) for f in written_files)
    assert total_lines == 10
