"""json_object_keys & json_array_length — structural inspection functions.

Demonstrates functions for inspecting the structure of JSON data without
fully parsing it: extracting keys from objects and measuring array lengths.

Key concepts:
    - json_object_keys(col) → array of key names
    - json_array_length(col) → integer count of array elements
    - Useful for data profiling and schema discovery
    - Works on JSON string columns (no schema needed)

Reference:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.json_object_keys.html
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.json_array_length.html
"""

from pyspark.sql import functions as F

from pys_json import (
    get_spark,
    print_dataframe,
    print_header,
    print_success,
    set_log_level,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.json_inspect")


if __name__ == "__main__":
    spark = get_spark("json-inspect")

    # =========================================================================
    # 1. json_object_keys — extract key names
    # =========================================================================
    print_header("1. json_object_keys")

    data_obj = [
        (1, '{"name": "Alice", "age": 30, "city": "NYC"}'),
        (2, '{"name": "Bob", "department": "Engineering"}'),
        (3, '{"x": 1, "y": 2, "z": 3, "w": 4}'),
    ]
    df_obj = spark.createDataFrame(data_obj, ["id", "json_str"])

    df_keys = df_obj.select(
        "id",
        F.json_object_keys("json_str").alias("keys"),
    )
    print_dataframe(df_keys, title="Object Keys")
    logger.info("Each row has different keys — useful for schema discovery")

    # Count keys per row
    df_key_count = df_obj.select(
        "id",
        F.size(F.json_object_keys("json_str")).alias("num_keys"),
    )
    print_dataframe(df_key_count, title="Key Count per Row")

    # =========================================================================
    # 2. json_array_length — count array elements
    # =========================================================================
    print_header("2. json_array_length")

    data_arr = [
        (1, "[1, 2, 3, 4, 5]"),
        (2, '["a", "b"]'),
        (3, "[]"),
        (4, '[{"x": 1}, {"x": 2}, {"x": 3}]'),
    ]
    df_arr = spark.createDataFrame(data_arr, ["id", "json_str"])

    df_len = df_arr.select(
        "id",
        "json_str",
        F.json_array_length("json_str").alias("length"),
    )
    print_dataframe(df_len, title="Array Lengths")

    # =========================================================================
    # 3. Profiling: discover variable schemas
    # =========================================================================
    print_header("3. Data Profiling with json_object_keys")

    data_mixed = [
        (1, '{"name": "Alice", "age": 30}'),
        (2, '{"name": "Bob", "email": "bob@co.com"}'),
        (3, '{"name": "Charlie", "age": 35, "email": "charlie@co.com", "phone": "555-0101"}'),
    ]
    df_mixed = spark.createDataFrame(data_mixed, ["id", "json_str"])

    # Explode keys to find all unique field names across records
    df_all_keys = (
        df_mixed.select(
            F.explode(F.json_object_keys("json_str")).alias("key"),
        )
        .distinct()
        .orderBy("key")
    )
    print_dataframe(df_all_keys, title="All Unique Keys Across Records")

    # Count how many records contain each key
    df_key_freq = (
        df_mixed.select(F.explode(F.json_object_keys("json_str")).alias("key"))
        .groupBy("key")
        .agg(F.count("*").alias("occurrences"))
        .orderBy(F.col("occurrences").desc())
    )
    print_dataframe(df_key_freq, title="Key Frequency (schema coverage)")
    print_success("Profiling reveals which fields are optional vs always present")

    # =========================================================================
    # 4. Filter by structure
    # =========================================================================
    print_header("4. Filter by JSON Structure")

    # Find records with more than 2 keys
    df_complex = df_mixed.filter(
        F.size(F.json_object_keys("json_str")) > 2,
    )
    print_dataframe(df_complex, title="Records with > 2 Fields")

    # Find non-empty arrays
    df_nonempty = df_arr.filter(F.json_array_length("json_str") > 0)
    print_dataframe(df_nonempty, title="Non-Empty Arrays")
    print_success("Structure-based filtering without full JSON parsing")

    spark.stop()
