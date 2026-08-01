"""Multi-line JSON vs JSON Lines — handling both formats and mixed folders.

Demonstrates the difference between JSON Lines (one object per line) and
multi-line JSON (pretty-printed), when to use multiLine=true, and strategies
for folders containing a mix of both formats.

Key concepts:
    - Default: Spark expects JSON Lines (one object per line)
    - multiLine=true: reads entire file as one JSON value
    - multiLine=true cannot be used on JSON Lines files
    - Mixed folders require classification and separate processing
    - Array-rooted multi-line files need multiLine=true
    - Performance: multiLine files cannot be split across partitions

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

import os

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
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
logger = get_logger("example.multiline_vs_jsonlines")


if __name__ == "__main__":
    spark = get_spark("multiline-vs-jsonlines")
    base_dir = DATA_HOME + "/multiline_demo"
    os.makedirs(base_dir, exist_ok=True)

    # =========================================================================
    # 1. JSON Lines — default format
    # =========================================================================
    print_header("1. JSON Lines (Default — One Object Per Line)")

    jsonl_file = os.path.join(base_dir, "lines.json")
    write_json_lines(
        jsonl_file,
        [
            '{"id": 1, "name": "Alice", "skills": ["Spark", "Python"]}',
            '{"id": 2, "name": "Bob", "skills": ["Databricks", "SQL"]}',
            '{"id": 3, "name": "Charlie", "skills": ["Kafka", "Flink"]}',
        ],
    )
    print_path("JSON Lines file", jsonl_file)

    df_lines = spark.read.json(jsonl_file)
    print_dataframe(df_lines, title="JSON Lines — default read works")
    print_success("JSON Lines: one complete JSON object per line. Default read works perfectly.")

    # =========================================================================
    # 2. Multi-line JSON — single pretty-printed object
    # =========================================================================
    print_header("2. Multi-Line JSON (Pretty-Printed Object)")

    multiline_file = os.path.join(base_dir, "pretty.json")
    with open(multiline_file, "w") as f:
        f.write('{\n')
        f.write('  "id": 1,\n')
        f.write('  "name": "Mahfooz",\n')
        f.write('  "skills": ["Spark", "Databricks"]\n')
        f.write('}\n')
    print_path("Multi-line file", multiline_file)

    # Without multiLine — fails
    df_fail = spark.read.json(multiline_file)
    print_schema(df_fail, title="Without multiLine (corrupt)")
    logger.warning("Without multiLine: %s records, likely corrupt", df_fail.count())

    # With multiLine — works
    df_multi = spark.read.option("multiLine", "true").json(multiline_file)
    print_dataframe(df_multi, title="With multiLine=true — correct")
    print_success("Pretty-printed JSON requires multiLine=true")

    # =========================================================================
    # 3. Multi-line JSON array
    # =========================================================================
    print_header("3. Multi-Line JSON Array")

    array_file = os.path.join(base_dir, "array.json")
    with open(array_file, "w") as f:
        f.write('[\n')
        f.write('  {"id": 1, "name": "Alice"},\n')
        f.write('  {"id": 2, "name": "Bob"},\n')
        f.write('  {"id": 3, "name": "Charlie"}\n')
        f.write(']\n')
    print_path("Array-rooted file", array_file)

    schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
        ]
    )

    df_array = spark.read.option("multiLine", "true").schema(schema).json(array_file)
    print_dataframe(df_array, title="Array-rooted multi-line — multiLine=true")
    print_success("Array-rooted files: multiLine=true expands array elements into rows")

    # =========================================================================
    # 4. The mistake — multiLine on JSON Lines
    # =========================================================================
    print_header("4. Mistake — multiLine on JSON Lines File")

    # multiLine=true on a JSON Lines file reads only the first object
    df_wrong = spark.read.option("multiLine", "true").json(jsonl_file)
    print_dataframe(df_wrong, title="multiLine=true on JSON Lines (WRONG!)")
    logger.warning("Only got %s record instead of 3!", df_wrong.count())
    print_warning(
        "multiLine=true on JSON Lines reads the whole file as ONE JSON value — "
        "only the first valid object is parsed. Never mix up the modes!"
    )

    # =========================================================================
    # 5. Mixed folder — the hard case
    # =========================================================================
    print_header("5. Mixed Folder — Both Formats Together")

    mixed_dir = os.path.join(base_dir, "mixed")
    os.makedirs(mixed_dir, exist_ok=True)

    # JSON Lines file
    write_json_lines(
        os.path.join(mixed_dir, "batch1_lines.json"),
        [
            '{"id": 10, "name": "Line1"}',
            '{"id": 11, "name": "Line2"}',
        ],
    )

    # Pretty-printed file
    with open(os.path.join(mixed_dir, "batch2_pretty.json"), "w") as f:
        f.write('{\n  "id": 20,\n  "name": "Pretty1"\n}\n')

    # Array file
    with open(os.path.join(mixed_dir, "batch3_array.json"), "w") as f:
        f.write('[{"id": 30, "name": "Array1"}, {"id": 31, "name": "Array2"}]\n')

    print_path("Mixed folder", mixed_dir)

    # Neither mode works perfectly for all files
    df_default_mixed = spark.read.schema(schema).json(mixed_dir)
    logger.info("Default mode records: %s", df_default_mixed.count())
    print_dataframe(df_default_mixed, title="Default mode on mixed folder")

    df_multi_mixed = spark.read.option("multiLine", "true").schema(schema).json(mixed_dir)
    logger.info("multiLine mode records: %s", df_multi_mixed.count())
    print_dataframe(df_multi_mixed, title="multiLine=true on mixed folder")
    print_warning("Neither mode handles ALL formats in a mixed folder correctly")

    # =========================================================================
    # 6. Solution — classify and process separately
    # =========================================================================
    print_header("6. Solution — Classify Files by Format")

    # Read all files as text
    raw_files = spark.read.text(mixed_dir)
    raw_with_file = raw_files.withColumn("source_file", F.input_file_name())

    # Classify by file content patterns
    file_types = (
        raw_with_file.groupBy("source_file")
        .agg(
            F.first(F.trim(F.col("value"))).alias("first_line"),
            F.count("*").alias("line_count"),
        )
        .withColumn(
            "format",
            F.when(F.col("first_line").startswith("["), "array_multiline")
            .when(
                (F.col("first_line") == "{") & (F.col("line_count") > 1)
                & (~F.col("first_line").contains(",")),
                "object_multiline",
            )
            .otherwise("json_lines"),
        )
    )
    print_dataframe(file_types.select("source_file", "format"), title="File format classification")

    # Process each format correctly
    jsonl_files = [
        r["source_file"]
        for r in file_types.filter(F.col("format") == "json_lines").collect()
    ]
    multi_files = [
        r["source_file"]
        for r in file_types.filter(F.col("format").contains("multiline")).collect()
    ]

    results = []
    if jsonl_files:
        df_jl = spark.read.schema(schema).json(jsonl_files)
        results.append(df_jl)
        logger.info("JSON Lines files: %s records", df_jl.count())

    if multi_files:
        df_ml = spark.read.option("multiLine", "true").schema(schema).json(multi_files)
        results.append(df_ml)
        logger.info("Multi-line files: %s records", df_ml.count())

    # Union results
    if results:
        df_unified = results[0]
        for r in results[1:]:
            df_unified = df_unified.unionByName(r)
        print_dataframe(df_unified, title="Unified result (all formats)")
        logger.info("Total unified records: %s", df_unified.count())

    print_success("Classify files by format, process each group with correct mode, union results")

    # =========================================================================
    # 7. Performance implications
    # =========================================================================
    print_header("7. Performance Implications")

    perf_data = [
        ("JSON Lines", "Yes", "Yes", "Parallel read across partitions"),
        ("Multi-line (object)", "No", "No", "Single file = single task"),
        ("Multi-line (array)", "No", "No", "Entire array in memory"),
    ]
    df_perf = spark.createDataFrame(
        perf_data, ["Format", "Splittable", "Parallel", "Implication"]
    )
    print_dataframe(df_perf, title="Performance comparison")
    print_warning(
        "multiLine files cannot be split — entire file processed by one task. "
        "Prefer JSON Lines for large datasets."
    )

    # =========================================================================
    # 8. Best practices
    # =========================================================================
    print_header("8. Best Practices")

    practices = [
        ("Consistent format", "Separate /raw/json_lines/ from /raw/multiline/"),
        ("Prefer JSON Lines", "Splittable, parallel, better for large data"),
        ("multiLine for APIs", "API responses are often pretty-printed objects/arrays"),
        ("Classify mixed", "Read as text, detect format, process separately"),
        ("Convert early", "Normalize to JSON Lines or Parquet at bronze layer"),
    ]
    df_practices = spark.createDataFrame(practices, ["Practice", "Details"])
    print_dataframe(df_practices, title="Best Practices")
    print_success(
        "Keep formats separate. Use JSON Lines for data pipelines. "
        "Use multiLine=true only for API responses and config files."
    )

    spark.stop()
