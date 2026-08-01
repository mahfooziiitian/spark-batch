"""JSON with special characters in column names — handling dots, spaces, and reserved words.

Demonstrates how to read and process JSON with problematic field names that contain
dots, spaces, hyphens, or SQL reserved words, and how to normalize them.

Key concepts:
    - Backtick escaping for column references: col("`user.id`")
    - withColumnRenamed for individual renames
    - Bulk rename with toDF() or select+alias
    - Regex-based column normalization for production pipelines
    - Normalize immediately after bronze ingestion

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

import re

from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from pys_json import (
    DATA_HOME,
    get_spark,
    print_dataframe,
    print_header,
    print_path,
    print_schema,
    print_success,
    print_warning,
    set_log_level,
    write_json_lines,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.special_char_columns")


def normalize_column_name(name: str) -> str:
    """Convert any column name to a safe snake_case identifier."""
    # Replace dots, spaces, hyphens with underscores
    cleaned = re.sub(r"[.\s\-/]+", "_", name)
    # Remove any remaining non-alphanumeric characters (except underscore)
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "", cleaned)
    # Collapse multiple underscores
    cleaned = re.sub(r"_+", "_", cleaned)
    # Strip leading/trailing underscores and lowercase
    return cleaned.strip("_").lower()


if __name__ == "__main__":
    spark = get_spark("special-char-columns")

    # =========================================================================
    # 1. The problem — special characters in field names
    # =========================================================================
    print_header("1. Special Characters in Field Names")

    special_file = DATA_HOME + "/special_char_columns.json"
    write_json_lines(
        special_file,
        [
            '{"user.id": 1, "first name": "Mahfooz", "last-name": "Alam", "select": "admin", "data/path": "/tmp"}',
            '{"user.id": 2, "first name": "Alice", "last-name": "Smith", "select": "user", "data/path": "/home"}',
            '{"user.id": 3, "first name": "Bob", "last-name": "Jones", "select": "guest", "data/path": "/var"}',
        ],
    )
    print_path("Input", special_file)

    df = spark.read.json(special_file)
    print_schema(df, title="Schema with special characters")
    print_dataframe(df, title="Raw data")
    print_warning(
        "Column names contain dots, spaces, hyphens, reserved words, and slashes — "
        "all problematic for queries"
    )

    # =========================================================================
    # 2. Backtick escaping for queries
    # =========================================================================
    print_header("2. Backtick Escaping")

    df_selected = df.select(
        F.col("`user.id`"),
        F.col("`first name`"),
        F.col("`last-name`"),
        F.col("`select`"),
        F.col("`data/path`"),
    )
    print_dataframe(df_selected, title="Selected with backtick escaping")
    print_success("Wrap special column names in backticks: col(\"`user.id`\")")

    # =========================================================================
    # 3. Backticks in SQL expressions
    # =========================================================================
    print_header("3. Backticks in SQL")

    df.createOrReplaceTempView("special_table")
    df_sql = spark.sql("""
        SELECT
            `user.id` AS user_id,
            `first name` AS first_name,
            `last-name` AS last_name,
            `select` AS select_value
        FROM special_table
        WHERE `user.id` > 1
    """)
    print_dataframe(df_sql, title="SQL with backtick escaping")
    print_success("Backticks work in Spark SQL too — essential for reserved words like 'select'")

    # =========================================================================
    # 4. Individual rename with withColumnRenamed
    # =========================================================================
    print_header("4. withColumnRenamed")

    df_renamed = (
        df.withColumnRenamed("user.id", "user_id")
        .withColumnRenamed("first name", "first_name")
        .withColumnRenamed("last-name", "last_name")
        .withColumnRenamed("select", "select_value")
        .withColumnRenamed("data/path", "data_path")
    )
    print_schema(df_renamed, title="After individual renames")
    print_dataframe(df_renamed, title="Renamed columns")
    print_success("withColumnRenamed handles special chars without backticks in the old name")

    # =========================================================================
    # 5. Bulk rename with toDF()
    # =========================================================================
    print_header("5. Bulk Rename with toDF()")

    new_names = ["user_id", "first_name", "last_name", "data_path", "select_value"]
    df_bulk = df.toDF(*new_names)
    print_schema(df_bulk, title="After toDF() bulk rename")
    print_dataframe(df_bulk, title="Bulk renamed")
    print_warning("toDF() renames by position — ensure order matches df.columns exactly")

    # =========================================================================
    # 6. Regex-based automatic normalization
    # =========================================================================
    print_header("6. Automatic Column Normalization")

    logger.info("Original columns: %s", df.columns)

    normalized_names = [normalize_column_name(c) for c in df.columns]
    logger.info("Normalized columns: %s", normalized_names)

    df_normalized = df.toDF(*normalized_names)
    print_schema(df_normalized, title="Auto-normalized schema")
    print_dataframe(df_normalized, title="Auto-normalized data")
    print_success(
        "Regex normalization: replace [.\\s\\-/] with _, remove non-alphanumeric, lowercase"
    )

    # =========================================================================
    # 7. Select + alias pattern (most explicit)
    # =========================================================================
    print_header("7. Select + Alias Pattern")

    df_aliased = df.select(
        F.col("`user.id`").alias("user_id"),
        F.col("`first name`").alias("first_name"),
        F.col("`last-name`").alias("last_name"),
        F.col("`select`").alias("role"),
        F.col("`data/path`").alias("data_path"),
    )
    print_dataframe(df_aliased, title="Select + alias (most explicit)")
    print_success("Select + alias is safest — explicit mapping, no position dependency")

    # =========================================================================
    # 8. Nested special characters
    # =========================================================================
    print_header("8. Nested Structs with Special Characters")

    nested_file = DATA_HOME + "/special_char_nested.json"
    write_json_lines(
        nested_file,
        [
            '{"user info": {"first.name": "Alice", "last-name": "Smith"}, "meta data": {"created at": "2024-01-01"}}',
            '{"user info": {"first.name": "Bob", "last-name": "Jones"}, "meta data": {"created at": "2024-02-15"}}',
        ],
    )

    df_nested = spark.read.json(nested_file)
    print_schema(df_nested, title="Nested with special chars")

    # Access nested special-char fields
    df_nested_flat = df_nested.select(
        F.col("`user info`.`first.name`").alias("first_name"),
        F.col("`user info`.`last-name`").alias("last_name"),
        F.col("`meta data`.`created at`").alias("created_at"),
    )
    print_dataframe(df_nested_flat, title="Nested special chars — flattened")
    print_success("Chain backticks for nested access: col(\"`parent field`.`child.name`\")")

    # =========================================================================
    # 9. Production normalization function
    # =========================================================================
    print_header("9. Production Pattern — Normalize All Columns Recursively")

    from pyspark.sql import DataFrame

    def normalize_all_columns(df: DataFrame) -> DataFrame:
        """Normalize all column names in a DataFrame."""
        return df.toDF(*[normalize_column_name(c) for c in df.columns])

    df_prod = normalize_all_columns(df)
    print_dataframe(df_prod, title="Production-ready normalized DataFrame")
    print_success(
        "Best practice: normalize column names immediately after bronze ingestion — "
        "eliminates backtick hell in all downstream queries"
    )

    spark.stop()
