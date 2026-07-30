"""rescueDataColumn — Databricks / Spark Connect only.

Demonstrates the rescueDataColumn option which captures individual
type-mismatched field values into a dedicated column. This option is
available on Databricks and via Spark Connect — it is NOT supported
in open-source PySpark local mode.

Key concepts:
    - rescueDataColumn: captures mismatched field values (not whole rows)
    - Unlike _corrupt_record: rescues parseable rows with wrong types
    - The rescued column contains a JSON string of the mismatched values
    - Works with PERMISSIVE mode (default)
    - Requires Spark Connect or Databricks Runtime

Prerequisites:
    - A running Spark Connect server or Databricks cluster
    - Set SPARK_CONNECT_URL env var (default: sc://localhost:15002)
    - Start local server: ./sbin/start-connect-server.sh --packages \\
        org.apache.spark:spark-connect_2.12:4.0.0

Reference:
    https://docs.databricks.com/en/ingestion/file-repair.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pys_json import (
    get_spark_connect,
    print_dataframe,
    print_header,
    print_schema,
    print_success,
    print_warning,
    set_log_level,
    write_json_lines,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.rescue_data_connect")


if __name__ == "__main__":
    import os
    import tempfile

    out_dir = os.path.join(tempfile.gettempdir(), "pys_json", "rescue_connect")

    spark = get_spark_connect("rescue-data-connect")

    # =========================================================================
    # 1. Basic rescueDataColumn
    # =========================================================================
    print_header("1. rescueDataColumn — Basic Usage")

    data = [
        '{"name": "Alice", "age": 30}',
        '{"name": "Bob", "age": "twenty-five"}',
        '{"name": "Charlie", "age": 35}',
        '{"name": "Diana", "age": "N/A"}',
    ]

    data_file = os.path.join(out_dir, "basic.json")
    write_json_lines(data_file, data)

    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )

    df_rescued = (
        spark.read.option("mode", "PERMISSIVE")
        .option("rescueDataColumn", "_rescued_data")
        .schema(schema)
        .json(data_file)
    )
    print_schema(df_rescued, title="Schema with _rescued_data")
    print_dataframe(df_rescued, title="All Records")
    print_success("Type-mismatched 'age' values are captured in _rescued_data")

    # =========================================================================
    # 2. Filtering rescued rows
    # =========================================================================
    print_header("2. Inspecting Rescued Values")

    df_has_rescued = df_rescued.filter(F.col("_rescued_data").isNotNull())
    print_dataframe(df_has_rescued, title="Rows with Rescued Data")
    logger.info("Found %s rows with type-mismatched values", df_has_rescued.count())

    df_clean = df_rescued.filter(F.col("_rescued_data").isNull())
    print_dataframe(df_clean, title="Clean Rows (no rescued data)")

    # =========================================================================
    # 3. rescueDataColumn vs _corrupt_record
    # =========================================================================
    print_header("3. rescueDataColumn vs _corrupt_record")

    mixed_data = [
        '{"name": "Alice", "age": 30}',
        '{"name": "Bob", "age": "old"}',
        "{TOTALLY BROKEN JSON}",
        '{"name": "Eve", "age": 22}',
    ]

    mixed_file = os.path.join(out_dir, "mixed.json")
    write_json_lines(mixed_file, mixed_data)

    # _corrupt_record captures syntax errors (whole row)
    schema_corrupt = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("_corrupt_record", StringType(), True),
        ]
    )

    df_corrupt = (
        spark.read.option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema_corrupt)
        .json(mixed_file)
        .cache()
    )
    print_dataframe(df_corrupt, title="_corrupt_record: whole-row capture")

    # rescueDataColumn captures type mismatches (individual fields)
    df_rescue = (
        spark.read.option("mode", "PERMISSIVE")
        .option("rescueDataColumn", "_rescued_data")
        .schema(schema)
        .json(mixed_file)
    )
    print_dataframe(df_rescue, title="rescueDataColumn: field-level capture")

    print_warning(
        "_corrupt_record → captures unparseable rows (syntax errors); "
        "rescueDataColumn → captures parseable rows with wrong types"
    )

    # =========================================================================
    # 4. Custom rescued column name
    # =========================================================================
    print_header("4. Custom Column Name")

    df_custom = (
        spark.read.option("mode", "PERMISSIVE").option("rescueDataColumn", "_bad_values").schema(schema).json(data_file)
    )
    print_schema(df_custom, title="Custom Column: _bad_values")
    print_dataframe(df_custom, title="Custom Rescued Column")
    print_success("Use any column name with rescueDataColumn option")

    # =========================================================================
    # 5. Multiple type mismatches in one row
    # =========================================================================
    print_header("5. Multiple Type Mismatches per Row")

    multi_data = [
        '{"name": "Alice", "age": 30, "score": 95.5}',
        '{"name": 123, "age": "old", "score": "high"}',
        '{"name": "Charlie", "age": 35, "score": 88.0}',
    ]

    multi_file = os.path.join(out_dir, "multi.json")
    write_json_lines(multi_file, multi_data)

    multi_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("score", IntegerType(), True),
        ]
    )

    df_multi = (
        spark.read.option("mode", "PERMISSIVE")
        .option("rescueDataColumn", "_rescued_data")
        .schema(multi_schema)
        .json(multi_file)
    )
    print_dataframe(df_multi, title="Multiple Mismatches per Row")
    print_success("All mismatched values from a single row are captured together in JSON format")

    # =========================================================================
    # 6. Schema evolution — extra fields rescued
    # =========================================================================
    print_header("6. Schema Evolution — Extra Fields")

    evolution_data = [
        '{"name": "Alice", "age": 30}',
        '{"name": "Bob", "age": 25, "email": "bob@example.com"}',
        '{"name": "Charlie", "age": 35, "phone": "555-0101", "dept": "eng"}',
    ]

    evolution_file = os.path.join(out_dir, "evolution.json")
    write_json_lines(evolution_file, evolution_data)

    # Schema only knows about name and age
    base_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )

    df_evolution = spark.read.option("rescueDataColumn", "_rescued_data").schema(base_schema).json(evolution_file)
    print_dataframe(df_evolution, title="Extra Fields Rescued")
    print_success(
        "Fields not in the schema (email, phone, dept) are captured in "
        "_rescued_data — no data loss during schema evolution"
    )

    spark.stop()
