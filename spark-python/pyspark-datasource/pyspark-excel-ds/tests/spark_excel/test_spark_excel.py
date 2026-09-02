"""Tests for pys_excel.spark_excel (format resolution helpers, no JVM package required)."""

import os

import pytest

from pys_excel.spark_excel import (
    CREALYTICS_EXCEL_FORMAT,
    NATIVE_EXCEL_FORMAT,
    SPARK_EXCEL_PACKAGE_SCALA_2_12,
    is_databricks_runtime,
    resolve_excel_format,
)


@pytest.fixture(autouse=True)
def _clear_databricks_env(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    yield


def test_is_databricks_runtime_absent_locally():
    assert is_databricks_runtime() is None


def test_is_databricks_runtime_present(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "15.4")
    assert is_databricks_runtime() == "15.4"


def test_resolve_format_defaults_to_crealytics_locally():
    assert resolve_excel_format() == CREALYTICS_EXCEL_FORMAT


def test_resolve_format_uses_crealytics_on_dbr_15(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "15.4")
    assert resolve_excel_format() == CREALYTICS_EXCEL_FORMAT


def test_resolve_format_uses_native_on_dbr_17_plus(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "17.1")
    assert resolve_excel_format() == NATIVE_EXCEL_FORMAT


def test_maven_package_coordinate_targets_spark_35():
    assert SPARK_EXCEL_PACKAGE_SCALA_2_12.startswith("com.crealytics:spark-excel_2.12:3.5")


def test_env_untouched_after_tests():
    assert "DATABRICKS_RUNTIME_VERSION" not in os.environ
