"""Tests for pys_excel.reader.ExcelReader."""

from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from pys_excel.reader import ExcelReader


def _write_sample_workbook(path: Path) -> None:
    employees = pd.DataFrame(
        {"emp_id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"], "salary": [95000.0, 72000.0, None]}
    )
    departments = pd.DataFrame({"department": ["Engineering", "Sales"], "manager": ["Frank", "Grace"]})
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        employees.to_excel(writer, sheet_name="Employees", index=False)
        departments.to_excel(writer, sheet_name="Departments", index=False)


def test_read_single_sheet(spark: SparkSession, tmp_path: Path) -> None:
    workbook = tmp_path / "sample.xlsx"
    _write_sample_workbook(workbook)

    df = ExcelReader(spark).sheet("Employees").read(str(workbook))

    assert df.count() == 3
    assert set(df.columns) == {"emp_id", "name", "salary"}


def test_read_handles_null_values(spark: SparkSession, tmp_path: Path) -> None:
    workbook = tmp_path / "sample.xlsx"
    _write_sample_workbook(workbook)

    df = ExcelReader(spark).sheet("Employees").read(str(workbook))
    rows = {row["name"]: row["salary"] for row in df.collect()}

    assert rows["Carol"] is None
    assert rows["Alice"] == 95000.0


def test_read_all_sheets(spark: SparkSession, tmp_path: Path) -> None:
    workbook = tmp_path / "sample.xlsx"
    _write_sample_workbook(workbook)

    sheets = ExcelReader(spark).read_all_sheets(str(workbook))

    assert set(sheets.keys()) == {"Employees", "Departments"}
    assert sheets["Departments"].count() == 2


def test_read_with_explicit_schema(spark: SparkSession, tmp_path: Path) -> None:
    workbook = tmp_path / "sample.xlsx"
    _write_sample_workbook(workbook)

    schema = StructType(
        [
            StructField("emp_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("salary", StringType(), True),
        ]
    )
    df = ExcelReader(spark).sheet("Employees").with_schema(schema).read(str(workbook))

    assert df.schema == schema


def test_fluent_options_are_immutable(spark: SparkSession) -> None:
    base = ExcelReader(spark)
    with_sheet = base.sheet("Employees")
    with_header = with_sheet.header(0)

    assert base.options == {}
    assert with_sheet.options == {"sheet_name": "Employees"}
    assert with_header.options == {"sheet_name": "Employees", "header": 0}
