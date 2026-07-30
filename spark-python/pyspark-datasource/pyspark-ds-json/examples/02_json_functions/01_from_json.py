"""from_json — parse JSON strings into structured columns.

Demonstrates from_json() for converting JSON string columns into
StructType, MapType, and ArrayType columns. The most commonly used
JSON function in PySpark.

Key concepts:
    - from_json(col, schema) → structured column
    - JSON string → StructType (known fields)
    - JSON string → MapType (dynamic keys)
    - JSON string → ArrayType (JSON arrays)
    - Schema can be StructType, DDL string, or schema_of_json()
    - Options: mode, dateFormat, timestampFormat

Signature:
    from_json(col, schema, options={}) → Column

Reference:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from pys_json import (
    get_spark,
    print_dataframe,
    print_header,
    print_schema,
    print_success,
    set_log_level,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.from_json")


if __name__ == "__main__":
    spark = get_spark("from-json")

    # =========================================================================
    # 1. JSON string → StructType
    # =========================================================================
    print_header("1. from_json → StructType")

    data = [
        (1, '{"name": "Alice", "age": 30}'),
        (2, '{"name": "Bob", "age": 25}'),
        (3, '{"name": "Charlie", "age": 35}'),
    ]
    df = spark.createDataFrame(data, ["id", "json_str"])

    schema = StructType(
        [
            StructField("name", StringType()),
            StructField("age", IntegerType()),
        ]
    )

    df_parsed = df.withColumn("parsed", F.from_json("json_str", schema))
    print_schema(df_parsed, title="from_json → Struct Schema")
    print_dataframe(df_parsed, title="Parsed Struct")

    # Access nested fields
    df_fields = df_parsed.select("id", "parsed.name", "parsed.age")
    print_dataframe(df_fields, title="Accessing Struct Fields")

    # =========================================================================
    # 2. JSON string → MapType (dynamic keys)
    # =========================================================================
    print_header("2. from_json → MapType")

    data_map = [
        (1, '{"Zipcode": "704", "City": "PARC PARQUE", "State": "PR"}'),
        (2, '{"Zipcode": "501", "City": "HOLTSVILLE", "State": "NY"}'),
    ]
    df_map = spark.createDataFrame(data_map, ["id", "json_str"])

    map_schema = MapType(StringType(), StringType())
    df_map_parsed = df_map.withColumn("parsed", F.from_json("json_str", map_schema))
    print_schema(df_map_parsed, title="from_json → Map Schema")
    print_dataframe(df_map_parsed, title="Parsed Map")

    # Access map values by key
    df_map_access = df_map_parsed.select(
        "id",
        F.col("parsed")["City"].alias("city"),
        F.col("parsed")["State"].alias("state"),
    )
    print_dataframe(df_map_access, title="Map Value Access")
    print_success("MapType is ideal when JSON keys are unknown at schema definition time")

    # =========================================================================
    # 3. JSON string → ArrayType (JSON arrays)
    # =========================================================================
    print_header("3. from_json → ArrayType")

    data_arr = [
        (1, '[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]'),
        (2, '[{"name": "Charlie", "age": 35}]'),
    ]
    df_arr = spark.createDataFrame(data_arr, ["id", "json_str"])

    arr_schema = ArrayType(
        StructType(
            [
                StructField("name", StringType()),
                StructField("age", IntegerType()),
            ]
        )
    )

    df_arr_parsed = df_arr.withColumn("parsed", F.from_json("json_str", arr_schema))
    print_schema(df_arr_parsed, title="from_json → Array Schema")
    print_dataframe(df_arr_parsed, title="Parsed Array")

    # Explode array into rows
    df_exploded = df_arr_parsed.select("id", F.explode("parsed").alias("person"))
    df_exploded = df_exploded.select("id", "person.name", "person.age")
    print_dataframe(df_exploded, title="Exploded Array Elements")

    # =========================================================================
    # 4. Using DDL string as schema
    # =========================================================================
    print_header("4. from_json with DDL String Schema")

    df_ddl = df.withColumn(
        "parsed",
        F.from_json("json_str", "name STRING, age INT"),
    )
    print_dataframe(df_ddl, title="DDL String Schema")
    print_success("DDL strings are the most concise schema format for from_json")

    # =========================================================================
    # 5. from_json with options
    # =========================================================================
    print_header("5. from_json with Options")

    data_ts = [
        (1, '{"event": "login", "ts": "2024-03-15 10:30:00"}'),
        (2, '{"event": "logout", "ts": "2024-03-15 14:45:00"}'),
    ]
    df_ts = spark.createDataFrame(data_ts, ["id", "json_str"])

    ts_schema = "event STRING, ts TIMESTAMP"
    df_ts_parsed = df_ts.withColumn(
        "parsed",
        F.from_json("json_str", ts_schema, {"timestampFormat": "yyyy-MM-dd HH:mm:ss"}),
    )
    print_schema(df_ts_parsed, title="from_json with timestampFormat")
    print_dataframe(df_ts_parsed, title="Parsed with Options")

    # =========================================================================
    # 6. Handling malformed JSON
    # =========================================================================
    print_header("6. Malformed JSON Handling")

    data_bad = [
        (1, '{"name": "Alice", "age": 30}'),
        (2, "{bad json}"),
        (3, '{"name": "Charlie", "age": 35}'),
    ]
    df_bad = spark.createDataFrame(data_bad, ["id", "json_str"])

    df_bad_parsed = df_bad.withColumn("parsed", F.from_json("json_str", schema))
    print_dataframe(df_bad_parsed, title="Malformed JSON → null")
    print_success("from_json returns null for unparseable rows (PERMISSIVE by default)")

    spark.stop()
