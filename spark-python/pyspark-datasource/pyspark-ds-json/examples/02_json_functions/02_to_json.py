"""to_json — serialize structured columns to JSON strings.

Demonstrates to_json() for converting StructType, MapType, and ArrayType
columns back into JSON string columns. The inverse of from_json().

Key concepts:
    - to_json(col) → JSON string column
    - Works on structs, maps, arrays, and nested types
    - Options: dateFormat, timestampFormat, ignoreNullFields
    - Round-trip: from_json → transform → to_json

Signature:
    to_json(col, options={}) → Column

Reference:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_json.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pys_json import (
    get_spark,
    print_dataframe,
    print_header,
    print_success,
    set_log_level,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.to_json")


if __name__ == "__main__":
    spark = get_spark("to-json")

    # =========================================================================
    # 1. Struct column → JSON string
    # =========================================================================
    print_header("1. Struct → JSON String")

    df = spark.createDataFrame(
        [
            (1, "Alice", 30, "NYC"),
            (2, "Bob", 25, "LA"),
        ],
        ["id", "name", "age", "city"],
    )

    # Create a struct column, then serialize to JSON
    df_struct = df.withColumn(
        "person",
        F.struct("name", "age", "city"),
    )
    df_json = df_struct.withColumn("person_json", F.to_json("person"))
    print_dataframe(
        df_json.select("id", "person_json"),
        title="Struct → JSON String",
    )

    # =========================================================================
    # 2. Map column → JSON string
    # =========================================================================
    print_header("2. Map → JSON String")

    df_map = df.withColumn(
        "metadata",
        F.create_map(
            F.lit("name"),
            F.col("name"),
            F.lit("city"),
            F.col("city"),
        ),
    )
    df_map_json = df_map.withColumn("meta_json", F.to_json("metadata"))
    print_dataframe(
        df_map_json.select("id", "meta_json"),
        title="Map → JSON String",
    )

    # =========================================================================
    # 3. Round-trip: JSON string → struct → transform → JSON string
    # =========================================================================
    print_header("3. Round-Trip (from_json → transform → to_json)")

    data = [
        (1, '{"name": "Alice", "age": 30}'),
        (2, '{"name": "Bob", "age": 25}'),
    ]
    df_raw = spark.createDataFrame(data, ["id", "json_str"])

    schema = StructType(
        [
            StructField("name", StringType()),
            StructField("age", IntegerType()),
        ]
    )

    df_roundtrip = (
        df_raw.withColumn("parsed", F.from_json("json_str", schema))
        .withColumn("name_upper", F.upper(F.col("parsed.name")))
        .withColumn("age_plus_10", F.col("parsed.age") + 10)
        .withColumn(
            "modified",
            F.struct(
                F.col("name_upper").alias("name"),
                F.col("age_plus_10").alias("age"),
            ),
        )
        .withColumn("output_json", F.to_json("modified"))
    )
    print_dataframe(
        df_roundtrip.select("id", "json_str", "output_json"),
        title="Round-Trip: Input → Transform → Output",
    )

    # =========================================================================
    # 4. to_json with options (ignoreNullFields, dateFormat)
    # =========================================================================
    print_header("4. to_json with Options")

    df_nulls = spark.createDataFrame(
        [
            (1, "Alice", 30, None),
            (2, "Bob", None, "LA"),
        ],
        ["id", "name", "age", "city"],
    )

    df_with = df_nulls.withColumn("data", F.struct("name", "age", "city"))

    # Default: ignoreNullFields=true (nulls omitted)
    df_no_nulls = df_with.withColumn("json_default", F.to_json("data"))

    # Explicit: ignoreNullFields=false (nulls included)
    df_keep_nulls = df_no_nulls.withColumn(
        "json_with_nulls",
        F.to_json("data", {"ignoreNullFields": "false"}),
    )
    print_dataframe(
        df_keep_nulls.select("id", "json_default", "json_with_nulls"),
        title="Null Field Handling",
    )
    print_success("ignoreNullFields=true (default) produces smaller JSON")

    # =========================================================================
    # 5. Entire row to JSON
    # =========================================================================
    print_header("5. Entire Row → JSON")

    df_full = df.withColumn("row_json", F.to_json(F.struct("*")))
    print_dataframe(
        df_full.select("id", "row_json"),
        title="Full Row as JSON String",
    )
    print_success("struct('*') captures all columns into a single struct")

    # =========================================================================
    # 6. Struct back to JSON (Kafka producer pattern)
    # =========================================================================
    print_header("6. Struct → JSON for Kafka Producer")

    # Simulate a customer DataFrame (typical ETL output)
    customers = spark.createDataFrame(
        [
            (1, "Alice", "alice@co.com", "NYC", "premium"),
            (2, "Bob", "bob@co.com", "LA", "basic"),
            (3, "Charlie", "charlie@co.com", "Chicago", "premium"),
        ],
        ["id", "name", "email", "city", "tier"],
    )

    # Build the event struct, then serialize for Kafka
    df_kafka = customers.select(
        F.col("id").cast("string").alias("key"),
        F.to_json(
            F.struct("name", "email", "city", "tier"),
        ).alias("value"),
    )
    print_dataframe(df_kafka, title="Kafka Producer Format (key, value)")
    print_success("to_json(struct(...)) produces the 'value' column for Kafka writes")

    # =========================================================================
    # 7. Nested struct to JSON (preserves hierarchy)
    # =========================================================================
    print_header("7. Nested Struct → JSON")

    df_nested = spark.createDataFrame(
        [
            (1, "Alice", ("123 Main St", "NYC", "10001")),
            (2, "Bob", ("456 Oak Ave", "LA", "90001")),
        ],
        "id INT, name STRING, address STRUCT<street: STRING, city: STRING, zip: STRING>",
    )

    # Serialize the nested struct — hierarchy preserved in JSON
    df_nested_json = df_nested.withColumn(
        "customer_json",
        F.to_json(F.struct("name", "address")),
    )
    print_dataframe(
        df_nested_json.select("id", "customer_json"),
        title="Nested Struct Preserved in JSON",
    )

    # =========================================================================
    # 8. Array of structs to JSON
    # =========================================================================
    print_header("8. Array of Structs → JSON")

    df_orders = spark.createDataFrame(
        [
            (1, "ORD-001", [("Widget", 3, 9.99), ("Gadget", 1, 29.99)]),
            (2, "ORD-002", [("Bolt", 100, 0.10)]),
        ],
        "id INT, order_id STRING, items ARRAY<STRUCT<product: STRING, qty: INT, price: DOUBLE>>",
    )

    df_orders_json = df_orders.withColumn(
        "items_json",
        F.to_json("items"),
    )
    print_dataframe(
        df_orders_json.select("order_id", "items_json"),
        title="Array of Structs → JSON Array",
    )
    print_success("Arrays and nested structs serialize naturally to JSON arrays/objects")

    # =========================================================================
    # 9. to_json with timestamp formatting
    # =========================================================================
    print_header("9. Timestamp Formatting in to_json")

    df_events = spark.createDataFrame(
        [
            (1, "login", "2024-03-15 10:30:00"),
            (2, "logout", "2024-03-15 14:45:00"),
        ],
        ["id", "event", "ts"],
    ).withColumn("ts", F.to_timestamp("ts"))

    # Default timestamp format
    df_ts_default = df_events.withColumn(
        "json_default",
        F.to_json(F.struct("event", "ts")),
    )

    # Custom timestamp format
    df_ts_custom = df_ts_default.withColumn(
        "json_custom",
        F.to_json(F.struct("event", "ts"), {"timestampFormat": "yyyy/MM/dd HH:mm"}),
    )
    print_dataframe(
        df_ts_custom.select("id", "json_default", "json_custom"),
        title="Timestamp Format Options",
    )
    print_success("Use timestampFormat option for downstream system compatibility")

    spark.stop()
