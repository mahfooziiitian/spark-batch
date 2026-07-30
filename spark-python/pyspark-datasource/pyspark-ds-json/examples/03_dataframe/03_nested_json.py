"""Nested JSON — working with structs, arrays, and maps in DataFrames.

Demonstrates accessing, transforming, and flattening nested JSON structures
using DataFrame operations and built-in functions.

Key concepts:
    - Dot notation for struct field access (address.city)
    - explode() / explode_outer() for arrays
    - getItem() for map access
    - Flattening nested structures into flat columns
    - Creating nested structs from flat columns (struct())
    - collect_list / collect_set for re-aggregating

Reference:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
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
    DATA_HOME,
    get_spark,
    print_dataframe,
    print_header,
    print_path,
    print_schema,
    print_success,
    set_log_level,
    write_json_lines,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.nested_json")


if __name__ == "__main__":
    spark = get_spark("nested-json-dataframe")

    # =========================================================================
    # 1. Struct field access (dot notation)
    # =========================================================================
    print_header("1. Struct Field Access (dot notation)")

    nested_schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField(
                "address",
                StructType(
                    [
                        StructField("street", StringType()),
                        StructField("city", StringType()),
                        StructField("state", StringType()),
                    ]
                ),
            ),
        ]
    )

    nested_file = DATA_HOME + "/file_data/json/df_demo/nested_people.json"
    write_json_lines(
        nested_file,
        [
            '{"id": 1, "name": "Alice", "address": {"street": "123 Main St", "city": "NYC", "state": "NY"}}',
            '{"id": 2, "name": "Bob", "address": {"street": "456 Oak Ave", "city": "LA", "state": "CA"}}',
            '{"id": 3, "name": "Charlie", "address": {"street": "789 Pine Rd", "city": "Chicago", "state": "IL"}}',
        ],
    )
    print_path("Input", nested_file)

    df_nested = spark.read.schema(nested_schema).json(nested_file)
    print_schema(df_nested, title="Nested Schema")

    df_flat = df_nested.select(
        "id",
        "name",
        F.col("address.street").alias("street"),
        F.col("address.city").alias("city"),
        F.col("address.state").alias("state"),
    )
    print_dataframe(df_flat, title="Flattened with Dot Notation")

    # =========================================================================
    # 2. Explode arrays
    # =========================================================================
    print_header("2. Explode Arrays")

    array_schema = StructType(
        [
            StructField("user", StringType()),
            StructField("skills", ArrayType(StringType())),
            StructField("scores", ArrayType(IntegerType())),
        ]
    )

    array_file = DATA_HOME + "/file_data/json/df_demo/arrays.json"
    write_json_lines(
        array_file,
        [
            '{"user": "Alice", "skills": ["Python", "Spark", "SQL"], "scores": [95, 87, 92]}',
            '{"user": "Bob", "skills": ["Java", "Scala"], "scores": [78, 85]}',
            '{"user": "Charlie", "skills": ["Python"], "scores": [90]}',
        ],
    )

    df_arr = spark.read.schema(array_schema).json(array_file)

    # explode creates one row per array element
    df_exploded = df_arr.select("user", F.explode("skills").alias("skill"))
    print_dataframe(df_exploded, title="Exploded Skills (one row per skill)")

    # posexplode includes the array index
    df_pos = df_arr.select("user", F.posexplode("scores").alias("idx", "score"))
    print_dataframe(df_pos, title="Posexplode Scores (with index)")

    # explode_outer keeps rows with null/empty arrays
    print_success("Use explode_outer() to retain rows with null arrays")

    # =========================================================================
    # 3. Array functions
    # =========================================================================
    print_header("3. Array Functions")

    df_arr_ops = df_arr.select(
        "user",
        F.size("skills").alias("num_skills"),
        F.array_contains("skills", "Python").alias("knows_python"),
        F.element_at("skills", 1).alias("first_skill"),
        F.sort_array("scores", asc=False).alias("scores_desc"),
    )
    print_dataframe(df_arr_ops, title="Array Functions")

    # =========================================================================
    # 4. Map access
    # =========================================================================
    print_header("4. Map Field Access")

    map_schema = StructType(
        [
            StructField("user", StringType()),
            StructField("config", MapType(StringType(), StringType())),
        ]
    )

    map_file = DATA_HOME + "/file_data/json/df_demo/maps.json"
    write_json_lines(
        map_file,
        [
            '{"user": "Alice", "config": {"theme": "dark", "lang": "en", "timezone": "EST"}}',
            '{"user": "Bob", "config": {"theme": "light", "lang": "fr"}}',
        ],
    )

    df_map = spark.read.schema(map_schema).json(map_file)

    # Access map values by key
    df_map_access = df_map.select(
        "user",
        F.col("config")["theme"].alias("theme"),
        F.col("config")["lang"].alias("language"),
        F.map_keys("config").alias("config_keys"),
        F.map_values("config").alias("config_values"),
    )
    print_dataframe(df_map_access, title="Map Access by Key")

    # Explode map into key-value rows
    df_map_exploded = df_map.select("user", F.explode("config").alias("key", "value"))
    print_dataframe(df_map_exploded, title="Exploded Map (key-value rows)")

    # =========================================================================
    # 5. Flatten deeply nested JSON
    # =========================================================================
    print_header("5. Flatten Deeply Nested JSON")

    deep_schema = StructType(
        [
            StructField("order_id", StringType()),
            StructField(
                "customer",
                StructType(
                    [
                        StructField("name", StringType()),
                        StructField(
                            "address",
                            StructType(
                                [
                                    StructField("city", StringType()),
                                    StructField("zip", StringType()),
                                ]
                            ),
                        ),
                    ]
                ),
            ),
            StructField(
                "items",
                ArrayType(
                    StructType(
                        [
                            StructField("product", StringType()),
                            StructField("qty", IntegerType()),
                            StructField("price", IntegerType()),
                        ]
                    )
                ),
            ),
        ]
    )

    deep_file = DATA_HOME + "/file_data/json/df_demo/orders.json"
    write_json_lines(
        deep_file,
        [
            '{"order_id": "ORD-1", "customer": {"name": "Alice", "address": {"city": "NYC", "zip": "10001"}}, "items": [{"product": "Widget", "qty": 2, "price": 10}, {"product": "Gadget", "qty": 1, "price": 25}]}',
            '{"order_id": "ORD-2", "customer": {"name": "Bob", "address": {"city": "LA", "zip": "90001"}}, "items": [{"product": "Widget", "qty": 5, "price": 10}]}',
        ],
    )

    df_deep = spark.read.schema(deep_schema).json(deep_file)
    print_schema(df_deep, title="Deeply Nested Schema")

    # Fully flatten
    df_flat_orders = df_deep.select(
        "order_id",
        F.col("customer.name").alias("customer_name"),
        F.col("customer.address.city").alias("customer_city"),
        F.col("customer.address.zip").alias("customer_zip"),
        F.explode("items").alias("item"),
    ).select(
        "order_id",
        "customer_name",
        "customer_city",
        "customer_zip",
        F.col("item.product"),
        F.col("item.qty"),
        F.col("item.price"),
        (F.col("item.qty") * F.col("item.price")).alias("line_total"),
    )
    print_dataframe(df_flat_orders, title="Fully Flattened Orders")

    # =========================================================================
    # 6. Re-nest flat columns into structs
    # =========================================================================
    print_header("6. Re-Nest: Flat → Struct")

    df_renested = df_flat.select(
        "id",
        "name",
        F.struct(
            F.col("street"),
            F.col("city"),
            F.col("state"),
        ).alias("address"),
    )
    print_schema(df_renested, title="Re-Nested Schema")
    print_dataframe(df_renested, title="Flat Columns → Struct")
    print_success("struct() creates nested columns from flat ones")

    spark.stop()
