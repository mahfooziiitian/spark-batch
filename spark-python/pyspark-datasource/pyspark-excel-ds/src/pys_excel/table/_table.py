"""Read Excel data into Spark tables, and write Spark tables back out to Excel.

This module implements the primary "Data Architect" workflow: land Excel
extracts as governed Spark tables (Delta by default), and produce Excel
reports/extracts from existing tables or ad-hoc SQL.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pys_excel._logging import get_logger
from pys_excel.reader import ExcelReader
from pys_excel.writer import ExcelWriter

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

logger = get_logger("table")


def excel_to_table(
    spark: SparkSession,
    path: str,
    table_name: str,
    *,
    sheet_name: str | int = 0,
    mode: str = "overwrite",
    file_format: str = "delta",
    schema: StructType | str | None = None,
    reader_options: dict[str, Any] | None = None,
    partition_by: list[str] | None = None,
) -> DataFrame:
    """Read an Excel sheet and persist it as a Spark table.

    Args:
        spark: Active SparkSession (Hive/Delta catalog support recommended).
        path: Path to the source .xlsx/.xls/.xlsm workbook.
        table_name: Destination table name (``catalog.schema.table`` or ``schema.table``).
        sheet_name: Sheet name or zero-based index to read.
        mode: Save mode — ``overwrite``, ``append``, ``ignore``, or ``error``.
        file_format: Table storage format (``delta`` recommended; also ``parquet``).
        schema: Optional explicit Spark schema applied to the source read.
        reader_options: Additional ``pandas.read_excel`` options (header, skiprows, etc.).
        partition_by: Optional column names to partition the table by.

    Returns:
        The DataFrame that was written, for further inspection/chaining.

    Example:
        >>> df = excel_to_table(spark, "employees.xlsx", "sales.employees", sheet_name="Employees")
        >>> spark.table("sales.employees").count()
    """
    reader = ExcelReader(spark).sheet(sheet_name)
    if schema is not None:
        reader = reader.with_schema(schema)
    if reader_options:
        reader = reader.with_options(**reader_options)

    logger.info("Loading Excel sheet %r from %s into table %s", sheet_name, path, table_name)
    df = reader.read(path)

    writer = df.write.format(file_format).mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(table_name)

    logger.info("Wrote %d rows to table %s (format=%s, mode=%s)", df.count(), table_name, file_format, mode)
    return df


def table_to_excel(
    spark: SparkSession,
    table_name: str,
    path: str,
    *,
    sheet_name: str = "Sheet1",
    query: str | None = None,
    writer_options: dict[str, Any] | None = None,
) -> None:
    """Read a Spark table (or arbitrary SQL query) and write it to an Excel workbook.

    Args:
        spark: Active SparkSession.
        table_name: Source table name. Ignored if ``query`` is provided.
        path: Destination .xlsx file path.
        sheet_name: Worksheet name to write.
        query: Optional SQL query to run instead of a plain ``SELECT * FROM table_name``.
        writer_options: Additional keyword args forwarded to :class:`~pys_excel.writer.ExcelWriter`
            fluent modifiers, e.g. ``{"index": True}``.

    Example:
        >>> table_to_excel(spark, "sales.employees", "reports/employees.xlsx")
    """
    sql = query or f"SELECT * FROM {table_name}"  # noqa: S608 - table_name is caller-controlled, not user input
    logger.info("Reading %s for Excel export to %s", table_name if not query else "custom query", path)
    df = spark.sql(sql)

    writer = ExcelWriter(sheet_name=sheet_name)
    if writer_options:
        for key, value in writer_options.items():
            setter = getattr(writer, f"with_{key}", None)
            writer = setter(value) if setter else writer.with_option(key, value)

    writer.write(df, path)
    logger.info("Exported %d rows to %s", df.count(), path)


def upsert_table_from_excel(
    spark: SparkSession,
    path: str,
    table_name: str,
    key_columns: list[str],
    *,
    sheet_name: str | int = 0,
    reader_options: dict[str, Any] | None = None,
) -> None:
    """Merge (upsert) an Excel sheet into an existing Delta table by key column(s).

    If the target table does not exist yet, it is created from the Excel data.
    Otherwise a ``MERGE INTO`` statement updates matching rows and inserts new ones.

    Args:
        spark: Active SparkSession with Delta Lake support.
        path: Path to the source .xlsx/.xls/.xlsm workbook.
        table_name: Target Delta table name.
        key_columns: Column names that uniquely identify a row (merge keys).
        sheet_name: Sheet name or zero-based index to read.
        reader_options: Additional ``pandas.read_excel`` options.

    Example:
        >>> upsert_table_from_excel(spark, "updates.xlsx", "sales.employees", key_columns=["emp_id"])
    """
    reader = ExcelReader(spark).sheet(sheet_name)
    if reader_options:
        reader = reader.with_options(**reader_options)
    df = reader.read(path)

    if not spark.catalog.tableExists(table_name):
        logger.info("Table %s does not exist; creating from Excel source", table_name)
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        return

    source_view = "_pys_excel_upsert_source"
    df.createOrReplaceTempView(source_view)

    on_clause = " AND ".join(f"target.{col} = source.{col}" for col in key_columns)
    update_cols = [c for c in df.columns if c not in key_columns]
    update_set = ", ".join(f"target.{col} = source.{col}" for col in update_cols)
    insert_cols = ", ".join(df.columns)
    insert_vals = ", ".join(f"source.{col}" for col in df.columns)

    merge_sql = f"""
        MERGE INTO {table_name} AS target
        USING {source_view} AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """  # noqa: S608 - identifiers are caller-controlled config, not user input
    logger.info("Merging %d rows from %s into %s on keys=%s", df.count(), path, table_name, key_columns)
    spark.sql(merge_sql)
    spark.catalog.dropTempView(source_view)
