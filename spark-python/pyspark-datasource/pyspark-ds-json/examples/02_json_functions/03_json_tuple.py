"""json_tuple — extract multiple JSON fields in one call.

Demonstrates json_tuple() for efficiently extracting multiple top-level
fields from a JSON string column. Faster than calling get_json_object()
multiple times because it only parses the JSON once.

Key concepts:
    - json_tuple(col, key1, key2, ...) → multiple columns
    - Only works on top-level keys (no nested paths)
    - Returns StringType for all extracted values
    - More efficient than multiple get_json_object calls
    - Must be used with select() (not withColumn)

Signature:
    json_tuple(col, *fields) → Column (generator-like, expands to N columns)

Reference:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.json_tuple.html
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
logger = get_logger("example.json_tuple")


if __name__ == "__main__":
    spark = get_spark("json-tuple")

    data = [
        (1, '{"Zipcode": 704, "ZipCodeType": "STANDARD", "City": "PARC PARQUE", "State": "PR"}'),
        (2, '{"Zipcode": 501, "ZipCodeType": "UNIQUE", "City": "HOLTSVILLE", "State": "NY"}'),
        (3, '{"Zipcode": 544, "ZipCodeType": "STANDARD", "City": "ADJUNTAS", "State": "PR"}'),
    ]
    df = spark.createDataFrame(data, ["id", "json_str"])
    print_dataframe(df, title="Raw Data")

    # =========================================================================
    # 1. Basic json_tuple — extract specific fields
    # =========================================================================
    print_header("1. Extract Multiple Fields")

    df_tuple = df.select(
        F.col("id"),
        F.json_tuple("json_str", "Zipcode", "City", "State"),
    ).toDF("id", "zipcode", "city", "state")

    print_dataframe(df_tuple, title="json_tuple Result")
    logger.info("All values are StringType regardless of source JSON type")

    # =========================================================================
    # 2. Extract subset of fields
    # =========================================================================
    print_header("2. Extract Subset")

    df_subset = df.select(
        F.col("id"),
        F.json_tuple("json_str", "City", "State"),
    ).toDF("id", "city", "state")

    print_dataframe(df_subset, title="City + State Only")

    # =========================================================================
    # 3. Missing keys return null
    # =========================================================================
    print_header("3. Missing Keys → null")

    df_missing = df.select(
        F.col("id"),
        F.json_tuple("json_str", "City", "Country", "Population"),
    ).toDF("id", "city", "country", "population")

    print_dataframe(df_missing, title="Non-existent Keys")
    print_success("Missing keys return null — no error thrown")

    # =========================================================================
    # 4. json_tuple vs get_json_object performance
    # =========================================================================
    print_header("4. json_tuple vs get_json_object")

    # json_tuple: parses JSON once, extracts all fields
    df_tuple_way = df.select(
        "id",
        F.json_tuple("json_str", "Zipcode", "City", "State"),
    ).toDF("id", "zipcode", "city", "state")

    # get_json_object: parses JSON once PER call (3 parses)
    df_gjo_way = df.select(
        "id",
        F.get_json_object("json_str", "$.Zipcode").alias("zipcode"),
        F.get_json_object("json_str", "$.City").alias("city"),
        F.get_json_object("json_str", "$.State").alias("state"),
    )

    print_dataframe(df_tuple_way, title="json_tuple (1 parse)")
    print_dataframe(df_gjo_way, title="get_json_object (3 parses)")
    print_success("json_tuple is faster when extracting multiple fields from the same JSON")

    spark.stop()
