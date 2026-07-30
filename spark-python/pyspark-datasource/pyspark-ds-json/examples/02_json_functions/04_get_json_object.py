"""get_json_object — extract values using JSONPath expressions.

Demonstrates get_json_object() for extracting a single value from a JSON
string column using JSONPath syntax. Supports nested access and array indexing.

Key concepts:
    - get_json_object(col, "$.path") → StringType column
    - JSONPath: $.field, $.nested.field, $.array[0]
    - Returns null for missing paths
    - Always returns StringType (cast if needed)
    - For multiple fields, prefer json_tuple() for performance

Signature:
    get_json_object(col, path) → Column

Reference:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.get_json_object.html
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
logger = get_logger("example.get_json_object")


if __name__ == "__main__":
    spark = get_spark("get-json-object")

    data = [
        (1, '{"name": "Alice", "address": {"city": "NYC", "zip": "10001"}, "scores": [95, 87, 92]}'),
        (2, '{"name": "Bob", "address": {"city": "LA", "zip": "90001"}, "scores": [78, 85]}'),
    ]
    df = spark.createDataFrame(data, ["id", "json_str"])

    # =========================================================================
    # 1. Top-level field access
    # =========================================================================
    print_header("1. Top-Level Field")

    df_name = df.select(
        "id",
        F.get_json_object("json_str", "$.name").alias("name"),
    )
    print_dataframe(df_name, title="$.name")

    # =========================================================================
    # 2. Nested field access
    # =========================================================================
    print_header("2. Nested Field (dot notation)")

    df_nested = df.select(
        "id",
        F.get_json_object("json_str", "$.address.city").alias("city"),
        F.get_json_object("json_str", "$.address.zip").alias("zip"),
    )
    print_dataframe(df_nested, title="$.address.city, $.address.zip")

    # =========================================================================
    # 3. Array indexing
    # =========================================================================
    print_header("3. Array Index Access")

    df_array = df.select(
        "id",
        F.get_json_object("json_str", "$.scores[0]").alias("first_score"),
        F.get_json_object("json_str", "$.scores[1]").alias("second_score"),
    )
    print_dataframe(df_array, title="$.scores[0], $.scores[1]")
    logger.info("Values are StringType — cast to IntegerType if needed")

    # =========================================================================
    # 4. Casting extracted values
    # =========================================================================
    print_header("4. Cast Extracted Values")

    df_cast = df.select(
        "id",
        F.get_json_object("json_str", "$.name").alias("name"),
        F.get_json_object("json_str", "$.scores[0]").cast("int").alias("first_score"),
    )
    print_dataframe(df_cast, title="Cast to IntegerType")

    # =========================================================================
    # 5. Missing paths return null
    # =========================================================================
    print_header("5. Missing Paths → null")

    df_missing = df.select(
        "id",
        F.get_json_object("json_str", "$.email").alias("email"),
        F.get_json_object("json_str", "$.address.country").alias("country"),
        F.get_json_object("json_str", "$.scores[99]").alias("score_99"),
    )
    print_dataframe(df_missing, title="Non-Existent Paths")
    print_success("Missing paths return null — no exceptions thrown")

    # =========================================================================
    # 6. Extract entire sub-object as JSON string
    # =========================================================================
    print_header("6. Extract Sub-Object as JSON")

    df_sub = df.select(
        "id",
        F.get_json_object("json_str", "$.address").alias("address_json"),
        F.get_json_object("json_str", "$.scores").alias("scores_json"),
    )
    print_dataframe(df_sub, title="Sub-Objects as JSON Strings")
    print_success("Nested objects/arrays returned as JSON strings for further processing")

    spark.stop()
