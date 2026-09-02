"""Distributed Excel I/O via the ``spark-excel`` (crealytics) data source.

The reader/writer/table modules in this package bridge through pandas, which
collects data to the Spark driver — fine for reporting-sized workbooks, but
not for cluster-scale Excel ingestion. This module instead drives Spark's
native ``DataFrameReader``/``DataFrameWriter`` ``.format(...)`` API against the
`spark-excel <https://github.com/crealytics/spark-excel>`_ data source, so
reads/writes are distributed across executors like any other Spark format.

Two data source formats are supported:

- ``com.crealytics.spark.excel`` — the community `spark-excel` connector.
  Works on any Spark 3.x/4.x cluster (OSS Spark, EMR, Databricks) once the
  Maven package is on the classpath. **Databricks Runtime 15.x (Spark 3.5)**
  is fully supported: attach ``com.crealytics:spark-excel_2.12:3.5.1_0.20.4``
  as a cluster Maven library, or pass it via ``spark.jars.packages`` for
  local/OSS clusters.
- ``excel`` — Databricks' **built-in** Excel data source (no library install
  required), available on **Databricks Runtime 17.1+**. Prefer this format
  when running on a sufficiently new Databricks Runtime.

Use :func:`resolve_excel_format` to pick the right format automatically based
on the active runtime.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from pys_excel._logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = get_logger("spark_excel")

#: Maven coordinate for the community spark-excel connector, matching Spark 3.5.x
#: (Databricks Runtime 13.3 LTS - 16.x) with Scala 2.12.
SPARK_EXCEL_PACKAGE_SCALA_2_12 = "com.crealytics:spark-excel_2.12:3.5.1_0.20.4"

#: Data source format string for the community connector.
CREALYTICS_EXCEL_FORMAT = "com.crealytics.spark.excel"

#: Data source format string for Databricks' built-in Excel connector (DBR 17.1+).
NATIVE_EXCEL_FORMAT = "excel"

#: Minimum Databricks Runtime major.minor version that ships built-in Excel support.
NATIVE_EXCEL_MIN_DBR = (17, 1)

#: Minimum Databricks Runtime major version validated for the crealytics connector
#: as a cluster-installed Maven library.
CREALYTICS_MIN_DBR_MAJOR = 15


def get_spark_with_excel_package(
    app_name: str = "pys-excel-spark-excel",
    master: str | None = None,
    package: str = SPARK_EXCEL_PACKAGE_SCALA_2_12,
    log_level: str = "WARN",
) -> SparkSession:
    """Create a local SparkSession with the spark-excel Maven package preloaded.

    Not needed on Databricks (attach the library to the cluster instead) — this
    is for local/OSS Spark runs that want to exercise the distributed
    ``com.crealytics.spark.excel`` format instead of the pandas-based reader.

    Args:
        app_name: Application name for Spark UI.
        master: Spark master URL. Defaults to SPARK_MASTER env var or local[*].
        package: Maven coordinate to load via ``spark.jars.packages``.
        log_level: Log level for SparkContext.

    Returns:
        SparkSession with the spark-excel package on the classpath.
    """
    from pyspark.sql import SparkSession

    resolved_master = master or os.environ.get("SPARK_MASTER", "local[*]")
    logger.info("Creating SparkSession with spark-excel package=%s", package)
    spark = (
        SparkSession.builder.appName(app_name)
        .master(resolved_master)
        .config("spark.jars.packages", package)
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(log_level)
    return spark


def is_databricks_runtime() -> str | None:
    """Return the Databricks Runtime version string if running on Databricks, else None."""
    return os.environ.get("DATABRICKS_RUNTIME_VERSION")


def resolve_excel_format(spark: SparkSession | None = None) -> str:
    """Pick the best available Excel data source format for the current runtime.

    Returns :data:`NATIVE_EXCEL_FORMAT` on Databricks Runtime 17.1+ (built-in,
    no library required), otherwise :data:`CREALYTICS_EXCEL_FORMAT` (requires
    the spark-excel Maven library on the classpath — pre-installed on the
    cluster for Databricks Runtime 15.x+, or via ``spark.jars.packages`` for
    OSS Spark).

    Args:
        spark: Unused; accepted for API symmetry / future runtime probing.

    Returns:
        The data source format string to pass to ``.format(...)``.
    """
    _ = spark
    dbr = is_databricks_runtime()
    if dbr:
        try:
            major, minor = (int(p) for p in dbr.split(".")[:2])
        except ValueError:
            major, minor = (0, 0)
        if (major, minor) >= NATIVE_EXCEL_MIN_DBR:
            logger.debug(
                "Databricks Runtime %s >= %s; using native '%s' format", dbr, NATIVE_EXCEL_MIN_DBR, NATIVE_EXCEL_FORMAT
            )
            return NATIVE_EXCEL_FORMAT
        logger.debug(
            "Databricks Runtime %s < %s; using '%s' format", dbr, NATIVE_EXCEL_MIN_DBR, CREALYTICS_EXCEL_FORMAT
        )
    return CREALYTICS_EXCEL_FORMAT


def read_spark_excel(
    spark: SparkSession,
    path: str,
    *,
    data_address: str = "'Sheet1'!A1",
    header: bool = True,
    infer_schema: bool = True,
    excel_format: str | None = None,
    options: dict[str, Any] | None = None,
) -> DataFrame:
    """Read an Excel workbook using Spark's native (distributed) Excel data source.

    Args:
        spark: Active SparkSession.
        path: Path to the workbook (local, DBFS, Volumes, or cloud storage URI).
        data_address: Cell range/sheet address, e.g. ``"'Sheet1'!A1"`` or
            ``"'Sheet1'!A1:F100"``. Use ``"'Sheet1'"`` to read the whole sheet.
        header: Whether the first row of the range is a header row.
        infer_schema: Infer column types by sampling data (slower, more accurate).
        excel_format: Force a specific format; defaults to :func:`resolve_excel_format`.
        options: Additional ``.option()`` key-value pairs (e.g. ``maxRowsInMemory``,
            ``workbookPassword``, ``timestampFormat``).

    Returns:
        DataFrame with the parsed Excel data, read in a distributed fashion.

    Example:
        >>> df = read_spark_excel(spark, "/Volumes/catalog/schema/vol/employees.xlsx",
        ...                        data_address="'Employees'!A1")
    """
    fmt = excel_format or resolve_excel_format(spark)
    logger.info("Reading Excel via format=%s path=%s data_address=%s", fmt, path, data_address)

    reader = spark.read.format(fmt).option("header", str(header).lower())
    if fmt == CREALYTICS_EXCEL_FORMAT:
        reader = reader.option("dataAddress", data_address).option("inferSchema", str(infer_schema).lower())
    else:
        # Native Databricks format uses headerRows / dataAddress option names.
        reader = reader.option("headerRows", "1" if header else "0").option("dataAddress", data_address)

    if options:
        for key, value in options.items():
            reader = reader.option(key, value)

    return reader.load(path)


def write_spark_excel(
    df: DataFrame,
    path: str,
    *,
    sheet_name: str = "Sheet1",
    header: bool = True,
    mode: str = "overwrite",
    excel_format: str | None = None,
    options: dict[str, Any] | None = None,
) -> None:
    """Write a DataFrame using Spark's native (distributed) Excel data source.

    Args:
        df: DataFrame to write.
        path: Destination path (local, DBFS, Volumes, or cloud storage URI).
        sheet_name: Worksheet name to create.
        header: Whether to write a header row.
        mode: Save mode — ``overwrite``, ``append``, ``ignore``, or ``error``.
        excel_format: Force a specific format; defaults to :func:`resolve_excel_format`.
        options: Additional ``.option()`` key-value pairs.

    Example:
        >>> write_spark_excel(df, "/Volumes/catalog/schema/vol/report.xlsx", sheet_name="Report")
    """
    fmt = excel_format or resolve_excel_format(df.sparkSession)
    logger.info("Writing Excel via format=%s path=%s sheet=%s mode=%s", fmt, path, sheet_name, mode)

    writer = df.write.format(fmt).mode(mode).option("header", str(header).lower())
    data_address = f"'{sheet_name}'!A1"
    if fmt == CREALYTICS_EXCEL_FORMAT:
        writer = writer.option("dataAddress", data_address)
    else:
        writer = writer.option("dataAddress", data_address)

    if options:
        for key, value in options.items():
            writer = writer.option(key, value)

    writer.save(path)
