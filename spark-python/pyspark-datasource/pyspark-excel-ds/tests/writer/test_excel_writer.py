"""Tests for pys_excel.writer.ExcelWriter."""

from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession

from pys_excel.writer import ExcelWriter


def test_write_single_sheet(spark: SparkSession, tmp_path: Path) -> None:
    df = spark.createDataFrame([(1, "Alice", 95000.0), (2, "Bob", 72000.0)], ["emp_id", "name", "salary"])
    out_path = tmp_path / "out.xlsx"

    ExcelWriter(sheet_name="Employees").write(df, str(out_path))

    assert out_path.exists()
    result = pd.read_excel(out_path, sheet_name="Employees")
    assert list(result.columns) == ["emp_id", "name", "salary"]
    assert len(result) == 2


def test_write_many_sheets(spark: SparkSession, tmp_path: Path) -> None:
    employees = spark.createDataFrame([(1, "Alice")], ["emp_id", "name"])
    departments = spark.createDataFrame([("Engineering", "Frank")], ["department", "manager"])
    out_path = tmp_path / "out.xlsx"

    ExcelWriter().write_many({"Employees": employees, "Departments": departments}, str(out_path))

    sheets = pd.read_excel(out_path, sheet_name=None)
    assert set(sheets.keys()) == {"Employees", "Departments"}


def test_write_with_openpyxl_engine(spark: SparkSession, tmp_path: Path) -> None:
    df = spark.createDataFrame([(1, "Alice")], ["emp_id", "name"])
    out_path = tmp_path / "out.xlsx"

    ExcelWriter().with_engine("openpyxl").write(df, str(out_path))

    result = pd.read_excel(out_path)
    assert len(result) == 1


def test_write_invalid_engine_raises(spark: SparkSession) -> None:
    import pytest

    with pytest.raises(ValueError, match="Unsupported engine"):
        ExcelWriter().with_engine("bogus")
