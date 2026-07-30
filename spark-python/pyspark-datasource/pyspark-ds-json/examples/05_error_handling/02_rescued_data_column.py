"""Corrupt record column — capture and inspect type-mismatched records.

Demonstrates advanced uses of _corrupt_record / columnNameOfCorruptRecord
beyond basic error modes: filtering, auditing, and recovering data from
records that fail type coercion.

Key concepts:
    - _corrupt_record captures the entire raw JSON line when any field fails
    - Use PERMISSIVE + cache() to filter corrupt vs valid records
    - Parse corrupt records with a relaxed (all-string) schema to recover data
    - columnNameOfCorruptRecord lets you rename the capture column
    - rescueDataColumn is Databricks-only (not available in OSS PySpark)

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pys_json import (
    get_spark,
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
logger = get_logger("example.corrupt_record")


if __name__ == "__main__":
    spark = get_spark("corrupt-record")

    import os
    import tempfile

    out_dir = os.path.join(tempfile.gettempdir(), "pys_json", "corrupt_record")

    # =========================================================================
    # 1. Capturing type-mismatch records
    # =========================================================================
    print_header("1. Type-Mismatch Records via _corrupt_record")

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
            StructField("_corrupt_record", StringType(), True),
        ]
    )

    df = (
        spark.read.option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .json(data_file)
        .cache()
    )
    print_schema(df, title="Schema with _corrupt_record")
    print_dataframe(df, title="All Records")
    print_success("Records where 'age' isn't an integer are captured as corrupt")

    # =========================================================================
    # 2. Separating valid from corrupt
    # =========================================================================
    print_header("2. Separating Valid from Corrupt Records")

    df_corrupt = df.filter(F.col("_corrupt_record").isNotNull())
    print_dataframe(df_corrupt, title="Corrupt Records (type mismatches)")
    logger.info("Found %s corrupt records out of %s total", df_corrupt.count(), df.count())

    df_valid = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
    print_dataframe(df_valid, title="Valid Records Only")
    print_success("Split your pipeline: route valid data forward, corrupt to a dead-letter queue")

    # =========================================================================
    # 3. Recovering data from corrupt records
    # =========================================================================
    print_header("3. Recovering Data from Corrupt Records")

    # Re-parse corrupt records with a relaxed all-string schema
    relaxed_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", StringType(), True),
        ]
    )

    df_recovered = df_corrupt.select(
        F.from_json("_corrupt_record", relaxed_schema).alias("recovered"),
    ).select("recovered.name", "recovered.age")
    print_dataframe(df_recovered, title="Recovered Data (all-string schema)")
    print_success("Re-parse corrupt JSON with relaxed types to recover usable data")

    # =========================================================================
    # 4. Custom corrupt record column name
    # =========================================================================
    print_header("4. Custom Column Name")

    schema_custom = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("_bad_rows", StringType(), True),
        ]
    )

    df_custom = (
        spark.read.option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_bad_rows")
        .schema(schema_custom)
        .json(data_file)
        .cache()
    )
    print_schema(df_custom, title="Custom Column: _bad_rows")
    print_dataframe(df_custom, title="Custom Corrupt Record Column")
    print_success("columnNameOfCorruptRecord lets you choose any column name")

    # =========================================================================
    # 5. Mixed errors: syntax + type mismatches
    # =========================================================================
    print_header("5. Mixed Errors (Syntax + Type Mismatches)")

    mixed_data = [
        '{"name": "Alice", "age": 30}',
        '{"name": "Bob", "age": "old"}',
        "{TOTALLY BROKEN JSON}",
        '{"name": "Eve", "age": 22}',
    ]

    mixed_file = os.path.join(out_dir, "mixed.json")
    write_json_lines(mixed_file, mixed_data)

    df_mixed = (
        spark.read.option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .json(mixed_file)
        .cache()
    )
    print_dataframe(df_mixed, title="Mixed Errors")

    df_mixed_corrupt = df_mixed.filter(F.col("_corrupt_record").isNotNull())
    print_dataframe(df_mixed_corrupt, title="All Corrupt (syntax + type)")
    print_warning(
        "_corrupt_record captures BOTH syntax errors and type mismatches — "
        "the entire raw line is stored regardless of error type"
    )

    # =========================================================================
    # 6. Databricks rescueDataColumn (note)
    # =========================================================================
    print_header("6. Databricks: rescueDataColumn (Not Available in OSS)")

    print_warning(
        "rescueDataColumn is a Databricks-only option that captures individual "
        "mismatched field values (not the whole row). It is NOT available in "
        "open-source Apache Spark."
    )

    logger.info("Databricks usage: .option('rescueDataColumn', '_rescued_data')")
    logger.info("OSS equivalent: use _corrupt_record + from_json with relaxed schema")
    print_success("In OSS PySpark, use _corrupt_record (section 1-3) to capture and recover type-mismatched data")

    spark.stop()
