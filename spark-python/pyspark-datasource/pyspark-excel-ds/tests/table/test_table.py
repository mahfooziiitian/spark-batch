"""Tests for pys_excel.table (Excel <-> Spark table bridge)."""

import importlib.util
from pathlib import Path

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from pys_excel.table import excel_to_table, table_to_excel, upsert_table_from_excel

DELTA_AVAILABLE = importlib.util.find_spec("delta") is not None


def _write_sample_workbook(path: Path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_excel(path, sheet_name="Employees", index=False, engine="openpyxl")


def test_excel_to_table(spark: SparkSession, tmp_path: Path) -> None:
    workbook = tmp_path / "employees.xlsx"
    _write_sample_workbook(workbook, [{"emp_id": 1, "name": "Alice"}, {"emp_id": 2, "name": "Bob"}])

    df = excel_to_table(
        spark, str(workbook), "default.pys_excel_test_employees", sheet_name="Employees", file_format="parquet"
    )

    assert df.count() == 2
    assert spark.table("default.pys_excel_test_employees").count() == 2
    spark.sql("DROP TABLE IF EXISTS default.pys_excel_test_employees")


def test_table_to_excel(spark: SparkSession, tmp_path: Path) -> None:
    spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["emp_id", "name"]).write.mode("overwrite").format(
        "parquet"
    ).saveAsTable("default.pys_excel_test_export")
    out_path = tmp_path / "export.xlsx"

    table_to_excel(spark, "default.pys_excel_test_export", str(out_path))

    result = pd.read_excel(out_path)
    assert len(result) == 2
    spark.sql("DROP TABLE IF EXISTS default.pys_excel_test_export")


@pytest.mark.skipif(not DELTA_AVAILABLE, reason="requires optional 'delta-spark' dependency")
def test_upsert_table_from_excel_creates_when_missing(spark: SparkSession, tmp_path: Path) -> None:
    """Uses the shared session-scoped ``spark`` fixture, which is Delta-enabled
    up front (see tests/conftest.py) whenever 'delta-spark' is installed."""
    workbook = tmp_path / "employees.xlsx"
    _write_sample_workbook(workbook, [{"emp_id": 1, "name": "Alice"}])

    upsert_table_from_excel(spark, str(workbook), "default.pys_excel_test_upsert", key_columns=["emp_id"])

    assert spark.table("default.pys_excel_test_upsert").count() == 1
    spark.sql("DROP TABLE IF EXISTS default.pys_excel_test_upsert")
