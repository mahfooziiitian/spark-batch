"""Reusable JSON reader with configurable options."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

from pys_json._logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

logger = get_logger("reader")


@dataclass
class JsonReader:
    """Configurable JSON file reader wrapping spark.read.json().

    Provides a fluent API for building JSON read configurations. All modifier
    methods return a new instance (immutable pattern).

    Args:
        spark: Active SparkSession.
        schema: Optional explicit schema. Skips inference when provided.
        options: Spark JSON read options (e.g., multiline, mode, encoding).

    Example:
        >>> reader = JsonReader(spark).multiline().permissive()
        >>> df = reader.read("/data/events.json")
    """

    spark: SparkSession
    schema: StructType | None = None
    options: dict[str, str] = field(default_factory=dict)

    def read(self, path: str | list[str]) -> DataFrame:
        """Read JSON from one or more paths.

        Args:
            path: File path, directory, glob, or list of paths.

        Returns:
            DataFrame with parsed JSON data.
        """
        logger.info(
            "Reading JSON from %s (options=%s, schema=%s)",
            path,
            self.options,
            "explicit" if self.schema else "inferred",
        )
        reader = self.spark.read

        if self.schema:
            reader = reader.schema(self.schema)

        for key, value in self.options.items():
            reader = reader.option(key, value)

        if isinstance(path, list):
            return reader.json(path)
        return reader.json(path)

    def read_text(self, json_lines: list[str]) -> DataFrame:
        """Read JSON directly from in-memory strings (useful for testing).

        Args:
            json_lines: List of JSON strings, one record per string.

        Returns:
            DataFrame with parsed JSON data.
        """
        logger.debug("Reading %d in-memory JSON lines", len(json_lines))
        rdd = self.spark.sparkContext.parallelize(json_lines)
        reader = self.spark.read

        if self.schema:
            reader = reader.schema(self.schema)

        for key, value in self.options.items():
            reader = reader.option(key, value)

        return reader.json(rdd)

    # --- Fluent modifiers (immutable) ---

    def with_option(self, key: str, value: str) -> JsonReader:
        """Return a new reader with an additional option set."""
        new_options = {**self.options, key: value}
        return JsonReader(spark=self.spark, schema=self.schema, options=new_options)

    def with_options(self, **kwargs: str) -> JsonReader:
        """Return a new reader with multiple options set at once."""
        new_options = {**self.options, **kwargs}
        return JsonReader(spark=self.spark, schema=self.schema, options=new_options)

    def with_schema(self, schema: StructType | str) -> JsonReader:
        """Return a new reader with the given schema (StructType or DDL string).

        Args:
            schema: StructType object or DDL string like "name STRING, age INT".
        """
        resolved: StructType
        if isinstance(schema, str):
            from pyspark.sql.types import _parse_datatype_string

            resolved = _parse_datatype_string(schema)  # type: ignore[assignment]
        else:
            resolved = schema
        return JsonReader(spark=self.spark, schema=resolved, options=self.options)

    # --- Read mode presets ---

    def multiline(self, enabled: bool = True) -> JsonReader:
        """Enable multiline mode for pretty-printed or array JSON files."""
        return self.with_option("multiline", str(enabled).lower())

    def permissive(self, corrupt_column: str = "_corrupt_record") -> JsonReader:
        """Configure PERMISSIVE mode — keeps malformed records in a designated column."""
        return self.with_option("mode", "PERMISSIVE").with_option("columnNameOfCorruptRecord", corrupt_column)

    def fail_fast(self) -> JsonReader:
        """Configure FAILFAST mode — throws exception on first malformed record."""
        return self.with_option("mode", "FAILFAST")

    def drop_malformed(self) -> JsonReader:
        """Configure DROPMALFORMED mode — silently drops unparseable rows."""
        return self.with_option("mode", "DROPMALFORMED")

    # --- Encoding ---

    def encoding(self, charset: str) -> JsonReader:
        """Set character encoding (UTF-8, UTF-16BE, UTF-16LE, UTF-32BE, UTF-32LE)."""
        return self.with_option("encoding", charset)

    def utf16_be(self) -> JsonReader:
        """Read UTF-16 Big Endian encoded JSON."""
        return self.encoding("UTF-16BE")

    def utf16_le(self) -> JsonReader:
        """Read UTF-16 Little Endian encoded JSON."""
        return self.encoding("UTF-16LE")

    # --- Schema inference options ---

    def primitives_as_string(self, enabled: bool = True) -> JsonReader:
        """Infer all primitive values as StringType."""
        return self.with_option("primitivesAsString", str(enabled).lower())

    def prefers_decimal(self, enabled: bool = True) -> JsonReader:
        """Infer floating-point values as DecimalType instead of DoubleType."""
        return self.with_option("prefersDecimal", str(enabled).lower())

    def sampling_ratio(self, ratio: float) -> JsonReader:
        """Set the fraction of data to sample for schema inference.

        Args:
            ratio: Float between 0.0 and 1.0. Default is 1.0 (sample all data).
        """
        return self.with_option("samplingRatio", str(ratio))

    def drop_all_null_fields(self, enabled: bool = True) -> JsonReader:
        """Drop fields that are all null during schema inference."""
        return self.with_option("dropFieldIfAllNull", str(enabled).lower())

    # --- Parsing options ---

    def allow_comments(self, enabled: bool = True) -> JsonReader:
        """Allow Java/C++ style comments in JSON."""
        return self.with_option("allowComments", str(enabled).lower())

    def allow_single_quotes(self, enabled: bool = True) -> JsonReader:
        """Allow single quotes for strings (default: true)."""
        return self.with_option("allowSingleQuotes", str(enabled).lower())

    def allow_unquoted_field_names(self, enabled: bool = True) -> JsonReader:
        """Allow unquoted JSON field names."""
        return self.with_option("allowUnquotedFieldNames", str(enabled).lower())

    def allow_numeric_leading_zeros(self, enabled: bool = True) -> JsonReader:
        """Allow leading zeros in numbers (e.g., 007)."""
        return self.with_option("allowNumericLeadingZeros", str(enabled).lower())

    def allow_non_numeric_numbers(self, enabled: bool = True) -> JsonReader:
        """Allow NaN, Infinity, -Infinity as valid numbers."""
        return self.with_option("allowNonNumericNumbers", str(enabled).lower())

    # --- Date/time formatting ---

    def date_format(self, pattern: str) -> JsonReader:
        """Set date format pattern (Java SimpleDateFormat)."""
        return self.with_option("dateFormat", pattern)

    def timestamp_format(self, pattern: str) -> JsonReader:
        """Set timestamp format pattern."""
        return self.with_option("timestampFormat", pattern)

    def timezone(self, tz_id: str) -> JsonReader:
        """Set timezone for timestamp parsing (e.g., 'UTC', 'America/New_York')."""
        return self.with_option("timeZone", tz_id)

    # --- Line handling ---

    def line_separator(self, sep: str) -> JsonReader:
        """Set line separator character(s)."""
        return self.with_option("lineSep", sep)

    def locale(self, locale_tag: str) -> JsonReader:
        """Set locale for number/date parsing (IETF BCP 47 tag, e.g., 'en-US')."""
        return self.with_option("locale", locale_tag)

    # --- Rescued data ---

    def rescued_data_column(self, column_name: str = "_rescued_data") -> JsonReader:
        """Enable rescued data column for schema evolution scenarios."""
        return self.with_option("rescuedDataColumn", column_name)
