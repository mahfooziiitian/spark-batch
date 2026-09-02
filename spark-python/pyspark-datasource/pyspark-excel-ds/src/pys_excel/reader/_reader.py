"""Reusable Excel reader with configurable options.

Spark has no built-in Excel data source, so this reader uses ``pandas.read_excel``
(via the ``openpyxl`` engine) to parse the workbook and then bridges the result
into a Spark DataFrame with ``spark.createDataFrame``. This keeps the project
self-contained — no JVM Excel package (e.g. ``com.crealytics:spark-excel``) is
required to run examples locally.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pandas as pd

from pys_excel._logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

logger = get_logger("reader")


def _pandas_to_spark(spark: SparkSession, pdf: pd.DataFrame, schema: StructType | None) -> DataFrame:
    """Convert a pandas DataFrame to a Spark DataFrame.

    ``spark.createDataFrame`` maps pandas' NaN/NaT sentinels to Spark ``null``
    per-column using each column's pandas dtype (important for datetime
    columns — blanket-casting to ``object`` beforehand breaks ``TimestampType``
    inference). Floating-point columns are a special case: missing numeric
    Excel cells surface as IEEE ``NaN`` rather than SQL ``NULL``, so those are
    normalized to ``null`` explicitly via ``nanvl``.

    When an explicit schema is supplied, the DataFrame is first created with
    inferred types and then ``cast()`` to the target schema column-by-column.
    This avoids strict type-matching failures from ``createDataFrame(..., schema=...)``
    (e.g. a whole-number salary column inferred as ``int`` cannot be handed
    directly to a ``DoubleType`` field) while still guaranteeing the final
    schema requested by the caller.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, FloatType

    df = spark.createDataFrame(pdf)
    df = df.select(
        [
            (F.when(F.isnan(f.name), None).otherwise(F.col(f.name)).alias(f.name))
            if isinstance(f.dataType, (DoubleType, FloatType))
            else F.col(f.name)
            for f in df.schema.fields
        ]
    )
    if schema is None:
        return df
    return df.select([F.col(field.name).cast(field.dataType).alias(field.name) for field in schema.fields])


@dataclass
class ExcelReader:
    """Configurable Excel file reader wrapping ``pandas.read_excel``.

    Provides a fluent API for building Excel read configurations. All modifier
    methods return a new instance (immutable pattern).

    Args:
        spark: Active SparkSession.
        schema: Optional explicit Spark schema. Skips inference when provided.
        options: ``pandas.read_excel`` keyword options (sheet_name, header, etc.).

    Example:
        >>> reader = ExcelReader(spark).sheet("Employees").header(0)
        >>> df = reader.read("/data/employees.xlsx")
    """

    spark: SparkSession
    schema: StructType | None = None
    options: dict[str, Any] = field(default_factory=dict)

    def read(self, path: str) -> DataFrame:
        """Read a single Excel sheet into a Spark DataFrame.

        Args:
            path: Path to the .xlsx/.xls/.xlsm workbook.

        Returns:
            DataFrame with the parsed sheet contents.
        """
        options = {**self.options}
        options.setdefault("sheet_name", 0)
        options.setdefault("engine", "openpyxl")
        logger.info(
            "Reading Excel from %s (options=%s, schema=%s)", path, options, "explicit" if self.schema else "inferred"
        )

        pdf = pd.read_excel(path, **options)
        if isinstance(pdf, dict):
            msg = "Multiple sheets were selected; use read_all_sheets() instead of read()."
            raise ValueError(msg)
        return _pandas_to_spark(self.spark, pdf, self.schema)

    def read_all_sheets(self, path: str) -> dict[str, DataFrame]:
        """Read every sheet in the workbook into a dict of Spark DataFrames.

        Args:
            path: Path to the .xlsx/.xls/.xlsm workbook.

        Returns:
            Mapping of sheet name to DataFrame.
        """
        options = {k: v for k, v in self.options.items() if k != "sheet_name"}
        options.setdefault("engine", "openpyxl")
        logger.info("Reading all Excel sheets from %s (options=%s)", path, options)

        sheets = pd.read_excel(path, sheet_name=None, **options)
        return {name: _pandas_to_spark(self.spark, pdf, self.schema) for name, pdf in sheets.items()}

    # --- Fluent modifiers (immutable) ---

    def with_option(self, key: str, value: Any) -> ExcelReader:
        """Return a new reader with an additional pandas read_excel option set."""
        new_options = {**self.options, key: value}
        return ExcelReader(spark=self.spark, schema=self.schema, options=new_options)

    def with_options(self, **kwargs: Any) -> ExcelReader:
        """Return a new reader with multiple options set at once."""
        new_options = {**self.options, **kwargs}
        return ExcelReader(spark=self.spark, schema=self.schema, options=new_options)

    def with_schema(self, schema: StructType | str) -> ExcelReader:
        """Return a new reader with the given Spark schema (StructType or DDL string).

        Args:
            schema: StructType object or DDL string like "name STRING, age INT".
        """
        resolved: StructType
        if isinstance(schema, str):
            from pyspark.sql.types import _parse_datatype_string

            resolved = _parse_datatype_string(schema)  # type: ignore[assignment]
        else:
            resolved = schema
        return ExcelReader(spark=self.spark, schema=resolved, options=self.options)

    # --- Sheet selection ---

    def sheet(self, name_or_index: str | int) -> ExcelReader:
        """Select a single sheet by name or zero-based index."""
        return self.with_option("sheet_name", name_or_index)

    # --- Header / row handling ---

    def header(self, row: int | None) -> ExcelReader:
        """Set the header row index (0-based). Use ``None`` for headerless sheets."""
        return self.with_option("header", row)

    def skiprows(self, n: int) -> ExcelReader:
        """Skip the first ``n`` rows before parsing headers/data."""
        return self.with_option("skiprows", n)

    def nrows(self, n: int) -> ExcelReader:
        """Limit the number of data rows read."""
        return self.with_option("nrows", n)

    def usecols(self, columns: str | list[str | int]) -> ExcelReader:
        """Restrict columns read — Excel-style range string (e.g. "A:C") or a list."""
        return self.with_option("usecols", columns)

    def names(self, column_names: list[str]) -> ExcelReader:
        """Override column names (use with ``header(None)`` for headerless sheets)."""
        return self.with_option("names", column_names)

    # --- Value handling ---

    def na_values(self, values: list[str]) -> ExcelReader:
        """Set additional strings to recognize as NA/NaN."""
        return self.with_option("na_values", values)

    def dtype(self, mapping: dict[str, Any]) -> ExcelReader:
        """Force pandas dtypes for specific columns before the Spark conversion."""
        return self.with_option("dtype", mapping)

    def keep_default_na(self, enabled: bool = True) -> ExcelReader:
        """Toggle pandas' default NA value recognition."""
        return self.with_option("keep_default_na", enabled)

    def engine(self, name: str) -> ExcelReader:
        """Set the pandas Excel engine (``openpyxl`` for .xlsx, ``xlrd`` for legacy .xls)."""
        return self.with_option("engine", name)
