"""Dynamic JSON keys — handling variable/unknown keys with MapType.

Demonstrates how to read JSON with dynamic keys using MapType schemas,
query specific keys, explode maps into rows, pivot maps into columns,
and handle nested dynamic structures.

Key concepts:
    - MapType schema for fields with variable/unknown keys
    - explode() converts map entries to rows (key, value columns)
    - Direct key access with bracket notation: metrics['cpu']
    - map_keys() and map_values() for introspection
    - pivot() to convert exploded maps back to wide format
    - Nested dynamic keys require MapType<STRING, MapType<...>>

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
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
    print_warning,
    set_log_level,
    write_json_lines,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.dynamic_json_keys")


if __name__ == "__main__":
    spark = get_spark("dynamic-json-keys")

    # =========================================================================
    # 1. Basic dynamic keys with MapType
    # =========================================================================
    print_header("1. Dynamic Keys with MapType")

    metrics_file = DATA_HOME + "/dynamic_keys_metrics.json"
    write_json_lines(
        metrics_file,
        [
            '{"id": 1, "metrics": {"cpu": 80, "memory": 65, "disk": 90}}',
            '{"id": 2, "metrics": {"gpu": 40, "network": 100}}',
            '{"id": 3, "metrics": {"cpu": 55, "memory": 70, "gpu": 95, "io_wait": 12}}',
        ],
    )
    print_path("Input", metrics_file)

    schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("metrics", MapType(StringType(), DoubleType()), True),
        ]
    )

    df = spark.read.schema(schema).json(metrics_file)
    print_schema(df, title="MapType schema")
    print_dataframe(df, title="Raw data with dynamic keys")
    print_success("MapType captures any key-value pairs regardless of which keys appear")

    # =========================================================================
    # 2. Querying specific keys
    # =========================================================================
    print_header("2. Querying Specific Keys")

    df_keys = df.select(
        "id",
        F.col("metrics")["cpu"].alias("cpu"),
        F.col("metrics")["memory"].alias("memory"),
        F.col("metrics")["gpu"].alias("gpu"),
    )
    print_dataframe(df_keys, title="Extract specific keys (null if missing)")
    print_success("Bracket notation returns null for keys that don't exist in a record")

    # =========================================================================
    # 3. Explode map to rows
    # =========================================================================
    print_header("3. Explode Map to Rows")

    df_exploded = df.select(
        "id",
        F.explode_outer("metrics").alias("metric_name", "metric_value"),
    )
    print_dataframe(df_exploded, title="Exploded map → one row per metric")
    logger.info("Total metric rows: %s", df_exploded.count())
    print_success("explode() on a map produces (key, value) columns — ideal for aggregation")

    # =========================================================================
    # 4. Map introspection — discover all keys
    # =========================================================================
    print_header("4. Discover All Keys in Dataset")

    all_keys = (
        df.select(F.explode(F.map_keys(F.col("metrics"))).alias("key"))
        .distinct()
        .orderBy("key")
    )
    print_dataframe(all_keys, title="All unique keys across all records")

    df_with_keys = df.select(
        "id",
        F.map_keys(F.col("metrics")).alias("available_keys"),
        F.size(F.col("metrics")).alias("num_metrics"),
    )
    print_dataframe(df_with_keys, title="Keys per record")
    print_success("map_keys() + explode + distinct reveals the full key universe")

    # =========================================================================
    # 5. Pivot — exploded map back to wide format
    # =========================================================================
    print_header("5. Pivot Map to Columns")

    df_pivot = (
        df_exploded.groupBy("id")
        .pivot("metric_name")
        .agg(F.first("metric_value"))
    )
    print_dataframe(df_pivot, title="Pivoted — each key becomes a column")
    print_warning(
        "Pivot creates one column per unique key — expensive with high cardinality. "
        "Consider filtering to known keys first."
    )

    # =========================================================================
    # 6. Filtering by key existence
    # =========================================================================
    print_header("6. Filter by Key Existence")

    df_has_gpu = df.filter(F.col("metrics")["gpu"].isNotNull())
    print_dataframe(df_has_gpu, title="Records that have 'gpu' metric")

    df_has_both = df.filter(
        F.col("metrics")["cpu"].isNotNull() & F.col("metrics")["memory"].isNotNull()
    )
    print_dataframe(df_has_both, title="Records with both 'cpu' and 'memory'")
    print_success("Filter on map keys using bracket notation + isNotNull()")

    # =========================================================================
    # 7. Aggregation on dynamic keys
    # =========================================================================
    print_header("7. Aggregation on Dynamic Keys")

    df_stats = df_exploded.groupBy("metric_name").agg(
        F.count("metric_value").alias("count"),
        F.avg("metric_value").alias("avg_value"),
        F.max("metric_value").alias("max_value"),
        F.min("metric_value").alias("min_value"),
    )
    print_dataframe(df_stats, title="Statistics per metric (across all records)")
    print_success("Explode first, then aggregate — works regardless of which keys exist")

    # =========================================================================
    # 8. Nested dynamic keys
    # =========================================================================
    print_header("8. Nested Dynamic Keys")

    nested_file = DATA_HOME + "/dynamic_keys_nested.json"
    write_json_lines(
        nested_file,
        [
            '{"id": 1, "config": {"database": {"host": "db1.local", "port": "5432"}, "cache": {"host": "redis.local", "port": "6379"}}}',
            '{"id": 2, "config": {"api": {"host": "api.prod", "port": "443"}, "queue": {"host": "rabbit.local", "port": "5672"}}}',
        ],
    )
    print_path("Input (nested dynamic)", nested_file)

    nested_schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField(
                "config",
                MapType(StringType(), MapType(StringType(), StringType())),
                True,
            ),
        ]
    )

    df_nested = spark.read.schema(nested_schema).json(nested_file)
    print_schema(df_nested, title="Nested MapType schema")

    # Explode outer map, then extract inner map fields
    df_services = df_nested.select(
        "id",
        F.explode_outer("config").alias("service_name", "service_config"),
    ).select(
        "id",
        F.col("service_name"),
        F.col("service_config")["host"].alias("host"),
        F.col("service_config")["port"].alias("port"),
    )
    print_dataframe(df_services, title="Nested maps flattened to service table")
    print_success("Nested MapType<STRING, MapType<STRING, STRING>> handles two levels of dynamic keys")

    # =========================================================================
    # 9. Without MapType — what goes wrong
    # =========================================================================
    print_header("9. Without MapType — Schema Inference Pitfall")

    # If you let Spark infer, it creates a StructType with fixed fields
    df_inferred = spark.read.json(metrics_file)
    print_schema(df_inferred, title="Inferred schema (fixed StructType!)")
    print_warning(
        "Without MapType, Spark infers a StructType from the union of all keys — "
        "new keys in future data won't be captured without schema changes"
    )

    print_success(
        "Always use MapType for dynamic keys — it adapts to any keys without schema changes"
    )

    spark.stop()
