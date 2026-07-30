"""Create DataFrames from JSON — all methods compared.

Demonstrates every way to create a PySpark DataFrame from JSON data:
in-memory strings, files, directories, globs, and Pandas bridge.

Key concepts:
    - spark.read.json() for files and directories
    - spark.createDataFrame() with explicit schema
    - RDD-based JSON parsing for in-memory data
    - Reading multiple files and glob patterns
    - Multiline JSON (pretty-printed / arrays)

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pys_json import (
    DATA_HOME,
    get_spark,
    print_dataframe,
    print_header,
    print_path,
    print_schema,
    print_success,
    set_log_level,
    write_json_file,
    write_json_lines,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.create_dataframe")


if __name__ == "__main__":
    spark = get_spark("create-dataframe-from-json")

    schema = StructType(
        [
            StructField("name", StringType(), nullable=False),
            StructField("age", IntegerType(), nullable=True),
            StructField("city", StringType(), nullable=True),
        ]
    )

    # =========================================================================
    # 1. From JSON Lines file (one JSON object per line)
    # =========================================================================
    print_header("1. From JSON Lines File")

    jsonl_file = DATA_HOME + "/file_data/json/df_demo/people.jsonl"
    write_json_lines(
        jsonl_file,
        [
            '{"name": "Alice", "age": 30, "city": "NYC"}',
            '{"name": "Bob", "age": 25, "city": "LA"}',
            '{"name": "Charlie", "age": 35, "city": "Chicago"}',
        ],
    )
    print_path("Input", jsonl_file)

    df1 = spark.read.schema(schema).json(jsonl_file)
    print_dataframe(df1, title="JSON Lines File")

    # =========================================================================
    # 2. From multiline JSON (pretty-printed)
    # =========================================================================
    print_header("2. From Multiline JSON (pretty-printed)")

    multi_file = DATA_HOME + "/file_data/json/df_demo/people_multi.json"
    write_json_file(
        multi_file,
        [
            {"name": "Alice", "age": 30, "city": "NYC"},
            {"name": "Bob", "age": 25, "city": "LA"},
        ],
        multiline=True,
    )
    print_path("Input", multi_file)

    df2 = spark.read.option("multiLine", True).schema(schema).json(multi_file)
    print_dataframe(df2, title="Multiline JSON")

    # =========================================================================
    # 3. From in-memory JSON strings (RDD)
    # =========================================================================
    print_header("3. From In-Memory JSON Strings")

    json_strings = [
        '{"name": "Diana", "age": 28, "city": "Seattle"}',
        '{"name": "Eve", "age": 32, "city": "Boston"}',
    ]
    rdd = spark.sparkContext.parallelize(json_strings)
    df3 = spark.read.schema(schema).json(rdd)
    print_dataframe(df3, title="In-Memory JSON (via RDD)")

    # =========================================================================
    # 4. From Python dicts (createDataFrame)
    # =========================================================================
    print_header("4. From Python Dicts")

    data = [
        {"name": "Frank", "age": 40, "city": "Denver"},
        {"name": "Grace", "age": 29, "city": "Austin"},
    ]
    df4 = spark.createDataFrame(data, schema=schema)
    print_dataframe(df4, title="Python Dicts → DataFrame")

    # =========================================================================
    # 5. From a directory of JSON files
    # =========================================================================
    print_header("5. From Directory (multiple files)")

    write_json_lines(
        DATA_HOME + "/file_data/json/df_demo/parts/part1.json",
        [
            '{"name": "Alice", "age": 30, "city": "NYC"}',
        ],
    )
    write_json_lines(
        DATA_HOME + "/file_data/json/df_demo/parts/part2.json",
        [
            '{"name": "Bob", "age": 25, "city": "LA"}',
        ],
    )
    write_json_lines(
        DATA_HOME + "/file_data/json/df_demo/parts/part3.json",
        [
            '{"name": "Charlie", "age": 35, "city": "Chicago"}',
        ],
    )
    print_path("Directory", DATA_HOME + "/file_data/json/df_demo/parts/")

    df5 = spark.read.schema(schema).json(DATA_HOME + "/file_data/json/df_demo/parts/")
    print_dataframe(df5, title="All Files in Directory")
    print_success(f"Read {df5.count()} rows from 3 files")

    # =========================================================================
    # 6. From a list of specific files
    # =========================================================================
    print_header("6. From Explicit File List")

    file_list = [
        DATA_HOME + "/file_data/json/df_demo/parts/part1.json",
        DATA_HOME + "/file_data/json/df_demo/parts/part3.json",
    ]
    df6 = spark.read.schema(schema).json(file_list)
    print_dataframe(df6, title="Explicit File List (part1 + part3)")

    # =========================================================================
    # 7. From glob pattern
    # =========================================================================
    print_header("7. From Glob Pattern")

    df7 = spark.read.schema(schema).json(DATA_HOME + "/file_data/json/df_demo/parts/part[12].json")
    print_dataframe(df7, title="Glob Pattern: part[12].json")

    # =========================================================================
    # 8. Empty DataFrame with JSON schema
    # =========================================================================
    print_header("8. Empty DataFrame with Schema")

    df_empty = spark.createDataFrame([], schema=schema)
    print_schema(df_empty, title="Empty DataFrame Schema")
    logger.info("Row count: %d", df_empty.count())
    print_success("Useful for schema validation and testing")

    spark.stop()
