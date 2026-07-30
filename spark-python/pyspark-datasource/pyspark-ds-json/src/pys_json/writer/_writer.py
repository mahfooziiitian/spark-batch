"""Reusable JSON writer with configurable options."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pys_json._logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = get_logger("writer")


# Supported compression codecs
COMPRESSION_CODECS = ("none", "gzip", "bzip2", "deflate", "lz4", "snappy", "zstd")


@dataclass
class JsonWriter:
    """Configurable JSON file writer wrapping DataFrame.write.json().

    Provides a fluent API for building JSON write configurations.

    Args:
        mode: Write mode — overwrite, append, ignore, error/errorifexists.
        compression: Compression codec (none, gzip, bzip2, deflate, lz4, snappy, zstd).
        options: Additional Spark JSON write options.

    Example:
        >>> writer = JsonWriter(compression="gzip").ignore_null_fields()
        >>> writer.write(df, "/output/events")
    """

    mode: str = "overwrite"
    compression: str = "none"
    options: dict[str, str] = field(default_factory=dict)

    def write(self, df: DataFrame, path: str) -> None:
        """Write a DataFrame as JSON to the given path.

        Args:
            df: DataFrame to write.
            path: Output directory path.
        """
        logger.info("Writing JSON to %s (mode=%s, compression=%s)", path, self.mode, self.compression)
        writer = df.write.mode(self.mode)
        writer = writer.option("compression", self.compression)

        for key, value in self.options.items():
            writer = writer.option(key, value)

        writer.json(path)
        logger.debug("Write complete: %s", path)

    def write_single_file(self, df: DataFrame, path: str) -> None:
        """Write DataFrame as a single JSON file (coalesces to 1 partition).

        Args:
            df: DataFrame to write.
            path: Output directory path (will contain one part file).
        """
        logger.info("Writing single JSON file to %s (compression=%s)", path, self.compression)
        writer = df.coalesce(1).write.mode(self.mode)
        writer = writer.option("compression", self.compression)

        for key, value in self.options.items():
            writer = writer.option(key, value)

        writer.json(path)

    def write_partitioned(self, df: DataFrame, path: str, *partition_cols: str) -> None:
        """Write DataFrame partitioned by specified columns.

        Args:
            df: DataFrame to write.
            path: Output base directory path.
            *partition_cols: Column names to partition by.
        """
        logger.info("Writing partitioned JSON to %s (partitions=%s)", path, partition_cols)
        writer = df.write.mode(self.mode)
        writer = writer.option("compression", self.compression)

        for key, value in self.options.items():
            writer = writer.option(key, value)

        writer.partitionBy(*partition_cols).json(path)

    # --- Fluent modifiers (immutable) ---

    def with_option(self, key: str, value: str) -> JsonWriter:
        """Return a new writer with an additional option set."""
        new_options = {**self.options, key: value}
        return JsonWriter(mode=self.mode, compression=self.compression, options=new_options)

    def with_mode(self, mode: str) -> JsonWriter:
        """Return a new writer with the specified write mode.

        Args:
            mode: overwrite, append, ignore, or error.
        """
        return JsonWriter(mode=mode, compression=self.compression, options=self.options)

    def with_compression(self, codec: str) -> JsonWriter:
        """Return a new writer with the specified compression codec.

        Args:
            codec: One of: none, gzip, bzip2, deflate, lz4, snappy, zstd.
        """
        if codec not in COMPRESSION_CODECS:
            msg = f"Unsupported codec '{codec}'. Use one of: {COMPRESSION_CODECS}"
            logger.error(msg)
            raise ValueError(msg)
        return JsonWriter(mode=self.mode, compression=codec, options=self.options)

    # --- Compression presets ---

    def gzip(self) -> JsonWriter:
        """Use gzip compression (good general-purpose compression ratio)."""
        return self.with_compression("gzip")

    def snappy(self) -> JsonWriter:
        """Use snappy compression (fast, Hadoop ecosystem standard)."""
        return self.with_compression("snappy")

    def zstd(self) -> JsonWriter:
        """Use zstd compression (excellent ratio + speed, Spark 4+)."""
        return self.with_compression("zstd")

    def lz4(self) -> JsonWriter:
        """Use lz4 compression (fastest decompression)."""
        return self.with_compression("lz4")

    def uncompressed(self) -> JsonWriter:
        """Write without compression."""
        return self.with_compression("none")

    # --- Formatting options ---

    def ignore_null_fields(self, enabled: bool = True) -> JsonWriter:
        """Omit fields with null values from output (default: true)."""
        return self.with_option("ignoreNullFields", str(enabled).lower())

    def date_format(self, pattern: str) -> JsonWriter:
        """Set date format for output (Java SimpleDateFormat pattern)."""
        return self.with_option("dateFormat", pattern)

    def timestamp_format(self, pattern: str) -> JsonWriter:
        """Set timestamp format for output."""
        return self.with_option("timestampFormat", pattern)

    def encoding(self, charset: str) -> JsonWriter:
        """Set output character encoding."""
        return self.with_option("encoding", charset)

    def line_separator(self, sep: str) -> JsonWriter:
        """Set line separator in output."""
        return self.with_option("lineSep", sep)
