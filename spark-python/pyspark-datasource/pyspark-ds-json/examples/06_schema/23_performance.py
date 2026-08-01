"""Large JSON files and performance — optimization strategies for production pipelines.

Demonstrates performance pitfalls when processing large or numerous JSON files and
the optimization techniques to address them.

Key concepts:
    - Schema inference reads all data twice (avoid in production)
    - Column pruning: select only needed fields early
    - Avoid repeated from_json calls on the same column
    - Convert to Parquet/Delta early (bronze → silver pattern)
    - Partition strategy for many small files (coalesce/repartition)
    - Predicate pushdown limitations with JSON
    - Cache parsed results when reused

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

import os
import tempfile
import time

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
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
logger = get_logger("example.performance")


if __name__ == "__main__":
    spark = get_spark("json-performance")
    out_dir = os.path.join(tempfile.gettempdir(), "pys_json", "performance")
    os.makedirs(out_dir, exist_ok=True)

    # =========================================================================
    # 1. Schema inference overhead
    # =========================================================================
    print_header("1. Schema Inference Overhead")

    perf_file = DATA_HOME + "/performance_large.json"
    records = [
        f'{{"id": {i}, "name": "user_{i}", "score": {i * 1.5}, "tags": ["t{i % 5}"], "meta": {{"region": "us-east-{i % 3}", "tier": "standard"}}}}'
        for i in range(20000)
    ]
    write_json_lines(perf_file, records)
    print_path("Test file (20K records)", perf_file)

    # With inference
    start = time.time()
    df_inferred = spark.read.json(perf_file)
    _ = df_inferred.count()
    infer_time = time.time() - start

    # With explicit schema
    schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField(
                "meta",
                StructType(
                    [
                        StructField("region", StringType(), True),
                        StructField("tier", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField("name", StringType(), True),
            StructField("score", DoubleType(), True),
            StructField("tags", ArrayType(StringType()), True),
        ]
    )

    start = time.time()
    df_explicit = spark.read.schema(schema).json(perf_file)
    _ = df_explicit.count()
    explicit_time = time.time() - start

    logger.info("With inference:       %.3fs", infer_time)
    logger.info("With explicit schema: %.3fs", explicit_time)
    logger.info("Speedup: %.1fx", infer_time / explicit_time if explicit_time > 0 else 0)
    print_success("Explicit schema skips the inference pass — always faster")

    # =========================================================================
    # 2. Column pruning — select only what you need
    # =========================================================================
    print_header("2. Column Pruning")

    # Bad: read all fields then filter
    start = time.time()
    df_all = spark.read.schema(schema).json(perf_file)
    _ = df_all.select("id", "name", "score", "tags", "meta").count()
    all_time = time.time() - start

    # Good: select only needed fields
    start = time.time()
    df_pruned = spark.read.schema(schema).json(perf_file).select("id", "score")
    _ = df_pruned.count()
    pruned_time = time.time() - start

    logger.info("All columns:    %.3fs", all_time)
    logger.info("Pruned (2 cols): %.3fs", pruned_time)
    print_dataframe(df_pruned.limit(3), title="Only selected columns read")
    print_success(
        "Select required fields immediately after read — "
        "reduces I/O and memory even though JSON doesn't support column pushdown"
    )

    # =========================================================================
    # 3. Avoid repeated from_json calls
    # =========================================================================
    print_header("3. Avoid Repeated from_json Parsing")

    payload_file = DATA_HOME + "/performance_payload.json"
    payload_records = [
        f'{{"id": {i}, "payload": "{{\\"type\\": \\"click\\", \\"x\\": {i}, \\"y\\": {i * 2}}}"}}' for i in range(5000)
    ]
    write_json_lines(payload_file, payload_records)

    payload_schema = StructType(
        [
            StructField("type", StringType(), True),
            StructField("x", IntegerType(), True),
            StructField("y", IntegerType(), True),
        ]
    )

    df_payload = spark.read.json(payload_file)

    # Bad: parse payload multiple times
    start = time.time()
    df_bad = df_payload.select(
        "id",
        F.from_json(F.col("payload"), payload_schema).getField("type").alias("type"),
        F.from_json(F.col("payload"), payload_schema).getField("x").alias("x"),
        F.from_json(F.col("payload"), payload_schema).getField("y").alias("y"),
    )
    _ = df_bad.count()
    bad_time = time.time() - start

    # Good: parse once, extract fields
    start = time.time()
    df_good = (
        df_payload.withColumn("parsed", F.from_json(F.col("payload"), payload_schema))
        .select("id", "parsed.type", "parsed.x", "parsed.y")
    )
    _ = df_good.count()
    good_time = time.time() - start

    logger.info("Repeated from_json (3x): %.3fs", bad_time)
    logger.info("Single from_json:        %.3fs", good_time)
    print_dataframe(df_good.limit(3), title="Parse once, extract multiple fields")
    print_success("Parse from_json ONCE, then use dot notation to access fields")

    # =========================================================================
    # 4. Many small files problem
    # =========================================================================
    print_header("4. Many Small Files Problem")

    small_dir = os.path.join(out_dir, "small_files")
    os.makedirs(small_dir, exist_ok=True)

    # Create 100 small files
    for i in range(100):
        write_json_lines(
            os.path.join(small_dir, f"file_{i:03d}.json"),
            [f'{{"id": {i}, "value": {i * 10}}}'],
        )

    # Read many small files
    start = time.time()
    df_small = spark.read.schema("id BIGINT, value BIGINT").json(small_dir)
    _ = df_small.count()
    small_time = time.time() - start

    # Coalesced single file
    single_file = os.path.join(out_dir, "single_large.json")
    all_records = [f'{{"id": {i}, "value": {i * 10}}}' for i in range(100)]
    write_json_lines(single_file, all_records)

    start = time.time()
    df_single = spark.read.schema("id BIGINT, value BIGINT").json(single_file)
    _ = df_single.count()
    single_time = time.time() - start

    logger.info("100 small files: %.3fs", small_time)
    logger.info("1 combined file: %.3fs", single_time)
    print_warning(
        "Many small files cause excessive task scheduling overhead. "
        "Coalesce or compact files for better performance."
    )

    # Solution: coalesce after reading
    df_coalesced = df_small.coalesce(1)
    df_coalesced.write.mode("overwrite").json(os.path.join(out_dir, "compacted"))
    print_success("Compact small files: read → coalesce(N) → write back")

    # =========================================================================
    # 5. Convert to Parquet/Delta early
    # =========================================================================
    print_header("5. Convert to Parquet Early (Bronze → Silver)")

    parquet_path = os.path.join(out_dir, "silver_parquet")

    # Write as Parquet
    start = time.time()
    df_explicit.write.mode("overwrite").option("compression", "none").parquet(parquet_path)
    write_time = time.time() - start

    # Read back from Parquet
    start = time.time()
    df_parquet = spark.read.parquet(parquet_path)
    _ = df_parquet.count()
    parquet_time = time.time() - start

    # Compare with JSON read
    start = time.time()
    _ = spark.read.schema(schema).json(perf_file).count()
    json_time = time.time() - start

    logger.info("JSON read:    %.3fs", json_time)
    logger.info("Parquet read: %.3fs", parquet_time)
    logger.info("Parquet write (one-time): %.3fs", write_time)
    print_success(
        "Convert JSON to Parquet at bronze→silver boundary. "
        "Parquet supports predicate pushdown, column pruning, and compression."
    )

    # =========================================================================
    # 6. Cache parsed results when reused
    # =========================================================================
    print_header("6. Cache Parsed DataFrames")

    df_parsed = spark.read.schema(schema).json(perf_file).cache()

    # First action materializes cache
    start = time.time()
    _ = df_parsed.count()
    first_time = time.time() - start

    # Second action uses cache
    start = time.time()
    _ = df_parsed.filter(F.col("score") > 100).count()
    second_time = time.time() - start

    # Third action uses cache
    start = time.time()
    _ = df_parsed.groupBy("meta.region").count().collect()
    third_time = time.time() - start

    logger.info("First action (materialize):  %.3fs", first_time)
    logger.info("Second action (from cache):  %.3fs", second_time)
    logger.info("Third action (from cache):   %.3fs", third_time)
    df_parsed.unpersist()
    print_success("Cache when the same DataFrame is used in multiple actions")

    # =========================================================================
    # 7. Array explosion impact
    # =========================================================================
    print_header("7. Array Explosion — Filter Before Explode")

    # Create data with arrays
    array_file = DATA_HOME + "/performance_arrays.json"
    array_records = [
        f'{{"id": {i}, "status": "{"active" if i % 2 == 0 else "inactive"}", "items": [{",".join(str(j) for j in range(10))}]}}'
        for i in range(5000)
    ]
    write_json_lines(array_file, array_records)

    array_schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("items", ArrayType(LongType()), True),
            StructField("status", StringType(), True),
        ]
    )
    df_arrays = spark.read.schema(array_schema).json(array_file)

    # Bad: explode then filter (processes all 50K rows then filters)
    start = time.time()
    df_explode_first = (
        df_arrays.select("id", "status", F.explode("items").alias("item"))
        .filter(F.col("status") == "active")
    )
    _ = df_explode_first.count()
    explode_first_time = time.time() - start

    # Good: filter then explode (filters 5K rows, explodes only 2.5K)
    start = time.time()
    df_filter_first = (
        df_arrays.filter(F.col("status") == "active")
        .select("id", F.explode("items").alias("item"))
    )
    _ = df_filter_first.count()
    filter_first_time = time.time() - start

    logger.info("Explode then filter: %.3fs (%s rows)", explode_first_time, df_explode_first.count())
    logger.info("Filter then explode: %.3fs (%s rows)", filter_first_time, df_filter_first.count())
    print_success("Filter BEFORE explode to reduce the number of rows being exploded")

    # =========================================================================
    # 8. Optimization checklist summary
    # =========================================================================
    print_header("8. Optimization Checklist")

    checklist = [
        ("Explicit schema", "Avoid inference (2x read)", "HIGH"),
        ("Column pruning", "Select only needed fields early", "HIGH"),
        ("Single from_json", "Parse once, extract multiple fields", "MEDIUM"),
        ("Compact files", "Coalesce many small files", "HIGH"),
        ("Convert to Parquet", "Bronze→Silver conversion", "HIGH"),
        ("Cache parsed DF", "When reused in multiple actions", "MEDIUM"),
        ("Filter before explode", "Reduce rows before array expansion", "MEDIUM"),
        ("Partition by key", "Enables predicate pushdown after conversion", "MEDIUM"),
    ]
    df_checklist = spark.createDataFrame(checklist, ["Optimization", "Description", "Impact"])
    print_dataframe(df_checklist, title="JSON Performance Optimization Checklist")
    print_success(
        "JSON is a landing format — optimize reads with explicit schema, "
        "then convert to columnar format (Parquet/Delta) for all downstream use"
    )

    spark.stop()
