"""Write DataFrames as JSON — output modes and formats.

Demonstrates writing PySpark DataFrames to JSON files with various
output configurations: write modes, compression, partitioning, and
single-file output.

Key concepts:
    - Write modes: overwrite, append, ignore, errorifexists
    - Compression codecs: gzip, snappy, zstd, lz4
    - Partitioned output by column values
    - Single-file output (coalesce)
    - ignoreNullFields option
    - Custom line separators and encodings

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option
"""

import os

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from pys_json import (
    DATA_HOME,
    get_spark,
    print_dataframe,
    print_header,
    print_path,
    print_success,
    set_log_level,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.write_json")

OUTPUT_DIR = DATA_HOME + "/df_demo/output"


if __name__ == "__main__":
    spark = get_spark("write-json")

    schema = StructType(
        [
            StructField("name", StringType()),
            StructField("department", StringType()),
            StructField("salary", DoubleType()),
            StructField("age", IntegerType()),
        ]
    )

    df = spark.createDataFrame(
        [
            ("Alice", "Engineering", 95000.0, 30),
            ("Bob", "Engineering", 85000.0, 25),
            ("Charlie", "Marketing", 75000.0, 35),
            ("Diana", "Marketing", 80000.0, 28),
            ("Eve", "Sales", 70000.0, 32),
        ],
        schema=schema,
    )

    # =========================================================================
    # 1. Basic write (overwrite mode)
    # =========================================================================
    print_header("1. Basic Write (overwrite)")

    out_basic = OUTPUT_DIR + "/basic"
    df.write.mode("overwrite").json(out_basic)
    print_path("Output", out_basic)

    df_read = spark.read.json(out_basic)
    print_dataframe(df_read, title="Read Back")
    print_success("Default write: one part file per partition")

    # =========================================================================
    # 2. Single file output
    # =========================================================================
    print_header("2. Single File Output")

    out_single = OUTPUT_DIR + "/single_file"
    df.coalesce(1).write.mode("overwrite").json(out_single)
    print_path("Output", out_single)

    part_files = [f for f in os.listdir(out_single) if f.startswith("part-")]
    logger.info("Part files: %s", part_files)
    print_success(f"Coalesced to {len(part_files)} file(s)")

    # =========================================================================
    # 3. Compressed output
    # =========================================================================
    print_header("3. Compressed Output")

    for codec in ["gzip", "snappy", "zstd"]:
        out_compressed = OUTPUT_DIR + f"/compressed_{codec}"
        df.write.mode("overwrite").option("compression", codec).json(out_compressed)
        print_path(f"  {codec}", out_compressed)

    print_success("Compressed files are smaller but still readable by spark.read.json")

    # =========================================================================
    # 4. Partitioned output
    # =========================================================================
    print_header("4. Partitioned by Column")

    out_partitioned = OUTPUT_DIR + "/partitioned"
    df.write.mode("overwrite").partitionBy("department").json(out_partitioned)
    print_path("Output", out_partitioned)

    # List partition directories
    partitions = sorted([d for d in os.listdir(out_partitioned) if d.startswith("department=")])
    logger.info("Partitions: %s", partitions)

    # Read back with partition discovery
    df_partitioned = spark.read.json(out_partitioned)
    print_dataframe(df_partitioned, title="Read Back (partition columns auto-discovered)")

    # =========================================================================
    # 5. Write modes comparison
    # =========================================================================
    print_header("5. Write Modes")

    out_modes = OUTPUT_DIR + "/modes"

    # overwrite — replaces existing data
    df.limit(2).write.mode("overwrite").json(out_modes)
    logger.info("overwrite: %d rows", spark.read.json(out_modes).count())

    # append — adds to existing data
    df.limit(1).write.mode("append").json(out_modes)
    logger.info("after append: %d rows", spark.read.json(out_modes).count())

    # ignore — skip if exists
    df.write.mode("ignore").json(out_modes)
    logger.info("after ignore: %d rows (unchanged)", spark.read.json(out_modes).count())

    print_success("overwrite=replace, append=add, ignore=skip, error=fail if exists")

    # =========================================================================
    # 6. ignoreNullFields
    # =========================================================================
    print_header("6. Null Field Handling")

    df_nulls = spark.createDataFrame(
        [
            ("Alice", "Engineering", 95000.0, None),
            ("Bob", None, 85000.0, 25),
        ],
        schema=schema,
    )

    out_with_nulls = OUTPUT_DIR + "/with_nulls"
    df_nulls.write.mode("overwrite").option("ignoreNullFields", "false").json(out_with_nulls)

    out_no_nulls = OUTPUT_DIR + "/no_nulls"
    df_nulls.write.mode("overwrite").option("ignoreNullFields", "true").json(out_no_nulls)

    logger.info("ignoreNullFields=false → null fields appear as null in JSON")
    logger.info("ignoreNullFields=true  → null fields omitted from JSON")
    print_success("ignoreNullFields=true produces smaller files")

    # =========================================================================
    # 7. Using JsonWriter from pys_json library
    # =========================================================================
    print_header("7. Using pys_json.JsonWriter")

    from pys_json import JsonWriter

    writer = JsonWriter(compression="gzip").ignore_null_fields()
    out_lib = OUTPUT_DIR + "/via_library"
    writer.write(df, out_lib)
    print_path("Output", out_lib)

    writer_partitioned = JsonWriter(compression="snappy")
    out_lib_part = OUTPUT_DIR + "/via_library_partitioned"
    writer_partitioned.write_partitioned(df, out_lib_part, "department")
    print_path("Partitioned", out_lib_part)
    print_success("JsonWriter provides a clean fluent API for writes")

    spark.stop()
