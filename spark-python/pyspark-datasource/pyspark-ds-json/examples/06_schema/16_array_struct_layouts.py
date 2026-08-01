"""Array of Struct vs Struct of Array — two JSON layouts and how to process each.

Demonstrates the differences between ARRAY<STRUCT<...>> and STRUCT<sku: ARRAY, qty: ARRAY>
layouts, including schema definitions, flattening strategies, and converting between them.

Key concepts:
    - Array of Struct: each element is a complete record (preferred for analytics)
    - Struct of Array: parallel arrays grouped under a struct (columnar layout)
    - explode works naturally on Array of Struct
    - Struct of Array requires posexplode + join or arrays_zip to correlate elements
    - arrays_zip converts Struct of Array → Array of Struct

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
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
logger = get_logger("example.array_struct_layouts")


if __name__ == "__main__":
    spark = get_spark("array-struct-layouts")

    # =========================================================================
    # 1. Array of Struct — the preferred layout
    # =========================================================================
    print_header("1. Array of Struct (Preferred)")

    aos_file = DATA_HOME + "/layout_array_of_struct.json"
    write_json_lines(
        aos_file,
        [
            '{"id": 1, "items": [{"sku": "A", "qty": 2}, {"sku": "B", "qty": 3}]}',
            '{"id": 2, "items": [{"sku": "C", "qty": 1}, {"sku": "D", "qty": 5}, {"sku": "E", "qty": 2}]}',
        ],
    )
    print_path("Input", aos_file)

    aos_schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField(
                "items",
                ArrayType(
                    StructType(
                        [
                            StructField("sku", StringType(), True),
                            StructField("qty", IntegerType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )

    df_aos = spark.read.schema(aos_schema).json(aos_file)
    print_schema(df_aos, title="Array of Struct schema")
    print_dataframe(df_aos, title="Raw data")

    # Flattening is straightforward with explode
    df_aos_flat = df_aos.select(
        "id",
        F.explode_outer("items").alias("item"),
    ).select(
        "id",
        F.col("item.sku").alias("sku"),
        F.col("item.qty").alias("qty"),
    )
    print_dataframe(df_aos_flat, title="Flattened with explode_outer")
    print_success("Array of Struct flattens naturally with explode — each element is a complete record")

    # =========================================================================
    # 2. Struct of Array — the columnar layout
    # =========================================================================
    print_header("2. Struct of Array (Columnar Layout)")

    soa_file = DATA_HOME + "/layout_struct_of_array.json"
    write_json_lines(
        soa_file,
        [
            '{"id": 1, "items": {"sku": ["A", "B"], "qty": [2, 3]}}',
            '{"id": 2, "items": {"sku": ["C", "D", "E"], "qty": [1, 5, 2]}}',
        ],
    )
    print_path("Input", soa_file)

    soa_schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField(
                "items",
                StructType(
                    [
                        StructField("sku", ArrayType(StringType()), True),
                        StructField("qty", ArrayType(IntegerType()), True),
                    ]
                ),
                True,
            ),
        ]
    )

    df_soa = spark.read.schema(soa_schema).json(soa_file)
    print_schema(df_soa, title="Struct of Array schema")
    print_dataframe(df_soa, title="Raw data")

    # Cannot simply explode — arrays are separate columns
    print_warning("Cannot use explode directly — sku and qty are separate parallel arrays")

    # =========================================================================
    # 3. Flattening Struct of Array with arrays_zip
    # =========================================================================
    print_header("3. Flatten Struct of Array — arrays_zip Approach")

    df_zipped = df_soa.select(
        "id",
        F.explode_outer(
            F.arrays_zip(F.col("items.sku"), F.col("items.qty"))
        ).alias("zipped"),
    ).select(
        "id",
        F.col("zipped.sku").alias("sku"),
        F.col("zipped.qty").alias("qty"),
    )
    print_dataframe(df_zipped, title="Flattened with arrays_zip + explode")
    print_success("arrays_zip correlates parallel arrays by position, then explode flattens")

    # =========================================================================
    # 4. Flattening Struct of Array with posexplode
    # =========================================================================
    print_header("4. Flatten Struct of Array — posexplode Approach")

    df_sku_pos = df_soa.select(
        "id",
        F.posexplode_outer(F.col("items.sku")).alias("pos", "sku"),
    )

    df_qty_pos = df_soa.select(
        "id",
        F.posexplode_outer(F.col("items.qty")).alias("pos", "qty"),
    )

    df_joined = df_sku_pos.join(df_qty_pos, on=["id", "pos"]).select("id", "sku", "qty")
    print_dataframe(df_joined, title="Flattened with posexplode + join")
    print_success("posexplode gives index position — join on (id, pos) to correlate arrays")

    # =========================================================================
    # 5. Converting Struct of Array → Array of Struct
    # =========================================================================
    print_header("5. Convert Struct of Array → Array of Struct")

    df_converted = df_soa.select(
        "id",
        F.arrays_zip(F.col("items.sku"), F.col("items.qty")).alias("items"),
    )
    print_schema(df_converted, title="Converted schema (now Array of Struct)")
    print_dataframe(df_converted, title="Converted data")
    print_success("arrays_zip() converts Struct-of-Array to Array-of-Struct in one step")

    # =========================================================================
    # 6. Converting Array of Struct → Struct of Array
    # =========================================================================
    print_header("6. Convert Array of Struct → Struct of Array")

    df_back = df_aos.select(
        "id",
        F.struct(
            F.transform(F.col("items"), lambda x: x.sku).alias("sku"),
            F.transform(F.col("items"), lambda x: x.qty).alias("qty"),
        ).alias("items"),
    )
    print_schema(df_back, title="Converted schema (now Struct of Array)")
    print_dataframe(df_back, title="Converted data")
    print_success("transform() extracts each field into its own array")

    # =========================================================================
    # 7. Comparison summary
    # =========================================================================
    print_header("7. Comparison Summary")

    comparison_data = [
        ("Array of Struct", "explode_outer", "Natural", "Analytics, flattening"),
        ("Struct of Array", "arrays_zip + explode", "Requires zip/join", "Columnar storage, APIs"),
    ]
    df_compare = spark.createDataFrame(
        comparison_data, ["Layout", "Flatten Method", "Ease", "Best For"]
    )
    print_dataframe(df_compare, title="Layout Comparison")
    print_success(
        "Array of Struct is preferred for analytics — "
        "use arrays_zip to convert from Struct of Array when needed"
    )

    spark.stop()
