"""JSON data validation utilities for quality checks and corrupt record analysis."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from pys_json._logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = get_logger("validation")


@dataclass
class ValidationResult:
    """Result of a JSON validation run.

    Attributes:
        total_rows: Total number of rows read.
        valid_rows: Number of rows without corrupt records.
        corrupt_rows: Number of rows with corrupt records.
        corrupt_df: DataFrame containing only the corrupt rows (if any).
    """

    total_rows: int
    valid_rows: int
    corrupt_rows: int
    corrupt_df: DataFrame | None

    @property
    def is_clean(self) -> bool:
        """Return True if no corrupt records were found."""
        return self.corrupt_rows == 0

    @property
    def corruption_rate(self) -> float:
        """Return the fraction of rows that are corrupt (0.0 to 1.0)."""
        if self.total_rows == 0:
            return 0.0
        return self.corrupt_rows / self.total_rows

    def summary(self) -> str:
        """Return a human-readable summary string."""
        status = "✓ CLEAN" if self.is_clean else "✗ HAS CORRUPT RECORDS"
        return (
            f"{status}\n"
            f"  Total rows:   {self.total_rows}\n"
            f"  Valid rows:   {self.valid_rows}\n"
            f"  Corrupt rows: {self.corrupt_rows} ({self.corruption_rate:.1%})"
        )


def validate_json(
    df: DataFrame,
    corrupt_column: str = "_corrupt_record",
) -> ValidationResult:
    """Validate a DataFrame read in PERMISSIVE mode.

    Counts valid vs corrupt records and returns a structured result.

    Args:
        df: DataFrame with a corrupt record column (read in PERMISSIVE mode).
        corrupt_column: Name of the corrupt record column.

    Returns:
        ValidationResult with counts and corrupt DataFrame.

    Example:
        >>> from pys_json import JsonReader
        >>> from pys_json.schema import with_corrupt_record
        >>> reader = JsonReader(spark).permissive().with_schema(with_corrupt_record(schema))
        >>> df = reader.read("data.json")
        >>> result = validate_json(df)
        >>> print(result.summary())
    """
    if corrupt_column not in df.columns:
        total = df.count()
        logger.info("Validation: no corrupt column '%s' found — all %d rows valid", corrupt_column, total)
        return ValidationResult(
            total_rows=total,
            valid_rows=total,
            corrupt_rows=0,
            corrupt_df=None,
        )

    total = df.count()
    corrupt_df = df.filter(F.col(corrupt_column).isNotNull())
    corrupt_count = corrupt_df.count()

    if corrupt_count > 0:
        logger.warning("Validation: %d/%d rows corrupt (%.1f%%)", corrupt_count, total, corrupt_count / total * 100)
    else:
        logger.info("Validation: all %d rows valid", total)

    return ValidationResult(
        total_rows=total,
        valid_rows=total - corrupt_count,
        corrupt_rows=corrupt_count,
        corrupt_df=corrupt_df if corrupt_count > 0 else None,
    )


def check_nulls(df: DataFrame, *columns: str) -> dict[str, int]:
    """Count null values in specified columns.

    Args:
        df: Source DataFrame.
        *columns: Column names to check. If empty, checks all columns.

    Returns:
        Dict mapping column names to their null counts.
    """
    cols_to_check = columns if columns else df.columns
    logger.debug("Checking nulls for columns: %s", cols_to_check)
    null_counts = df.select(
        *[F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in cols_to_check]
    ).collect()[0]
    return {c: null_counts[c] or 0 for c in cols_to_check}


def check_schema_match(df: DataFrame, expected_columns: dict[str, str]) -> list[str]:
    """Check if DataFrame columns match expected types.

    Args:
        df: DataFrame to validate.
        expected_columns: Dict of column_name -> expected_type_name (e.g., "string", "integer").

    Returns:
        List of mismatch descriptions. Empty list means all columns match.

    Example:
        >>> mismatches = check_schema_match(df, {"name": "string", "age": "integer"})
        >>> if mismatches:
        ...     print("Schema issues:", mismatches)
    """
    mismatches = []
    actual_types = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    logger.debug("Schema check: expected=%s actual=%s", expected_columns, actual_types)

    for col_name, expected_type in expected_columns.items():
        if col_name not in actual_types:
            mismatches.append(f"Missing column: '{col_name}'")
        elif expected_type.lower() not in actual_types[col_name].lower():
            mismatches.append(f"Column '{col_name}': expected '{expected_type}', got '{actual_types[col_name]}'")

    if mismatches:
        logger.warning("Schema mismatches found: %s", mismatches)
    return mismatches


def profile_json(df: DataFrame) -> DataFrame:
    """Generate a profiling summary for a JSON-derived DataFrame.

    Returns one row per column with: column_name, data_type, null_count, non_null_count,
    distinct_count (for string/int columns ≤ 1000 distinct values).

    Args:
        df: DataFrame to profile.

    Returns:
        DataFrame with profiling statistics.
    """
    spark = df.sparkSession
    stats = []
    logger.info("Profiling DataFrame with %d columns", len(df.schema.fields))

    total = df.count()
    for field in df.schema.fields:
        col_name = field.name
        null_count = df.filter(F.col(col_name).isNull()).count()
        non_null = total - null_count
        stats.append((col_name, field.dataType.simpleString(), null_count, non_null, total))

    return spark.createDataFrame(
        stats,
        schema=["column_name", "data_type", "null_count", "non_null_count", "total_rows"],
    )
