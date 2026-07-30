"""Compression codecs — write and read compressed JSON files.

Demonstrates all supported compression codecs for JSON output.
Spark auto-detects compression on read from file extensions.

Key concepts:
    - compression option: none, gzip, bzip2, deflate, lz4, snappy
    - Write: df.write.option("compression", codec).json(path)
    - Read: spark.read.json(path) — auto-detects codec
    - Codec trade-offs: speed vs ratio vs splittability

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession

from pys_json import (
    DATA_HOME,
    get_spark,
    print_dataframe,
    print_header,
    print_schema,
    print_success,
    print_warning,
    set_log_level,
    write_json_lines,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.compression")


def round_trip_codec(
    spark: SparkSession,
    df: DataFrame,
    out_dir: str,
    codec: str,
    title: str,
    *,
    show_schema: bool = False,
) -> bool:
    """Write JSON with a codec, read it back, and display the result."""
    codec_dir = os.path.join(out_dir, codec)
    logger.info("Writing codec=%s to %s", codec, codec_dir)

    try:
        df.write.mode("overwrite").option("compression", codec).json(codec_dir)
        df_read = spark.read.json(codec_dir).orderBy("id")

        if show_schema:
            print_schema(df_read, title="Baseline Schema")

        print_dataframe(df_read, title=title)
        return True
    except Exception as exc:  # pragma: no cover - environment-dependent codec availability
        print_warning(f"{codec} could not be demonstrated in this runtime ({type(exc).__name__}).")
        logger.warning("Codec %s failed: %s", codec, exc)
        return False


if __name__ == "__main__":
    spark = get_spark("compression-codecs")

    data = [
        {"id": 1, "name": "Alice", "city": "Seattle", "score": 98.5},
        {"id": 2, "name": "Bob", "city": "Austin", "score": 91.0},
        {"id": 3, "name": "Cara", "city": "Chicago", "score": 95.25},
    ]
    df = spark.createDataFrame(data)
    out_dir = os.path.join(DATA_HOME, "output", "examples", "compression")

    print_warning("Output is written under DATA_HOME/output so the example stays inside the project workspace.")
    logger.info("Compression demo output directory: %s", out_dir)

    # =========================================================================
    # 1. No compression (baseline)
    # =========================================================================
    print_header("1. No Compression (Baseline)")

    none_supported = round_trip_codec(
        spark,
        df,
        out_dir,
        "none",
        "Read Back: compression=none",
        show_schema=True,
    )

    # =========================================================================
    # 2. gzip
    # =========================================================================
    print_header("2. gzip")

    gzip_supported = round_trip_codec(spark, df, out_dir, "gzip", "Read Back: compression=gzip")
    if gzip_supported:
        logger.info("gzip is often chosen for the best compression ratio among common codecs")

    # =========================================================================
    # 3. bzip2
    # =========================================================================
    print_header("3. bzip2")

    bzip2_supported = round_trip_codec(spark, df, out_dir, "bzip2", "Read Back: compression=bzip2")
    if bzip2_supported:
        print_success("bzip2 is splittable, which is valuable for distributed reads on HDFS.")

    # =========================================================================
    # 4. deflate
    # =========================================================================
    print_header("4. deflate")

    deflate_supported = round_trip_codec(spark, df, out_dir, "deflate", "Read Back: compression=deflate")

    # =========================================================================
    # 5. lz4
    # =========================================================================
    print_header("5. lz4")

    lz4_supported = round_trip_codec(spark, df, out_dir, "lz4", "Read Back: compression=lz4")
    if lz4_supported:
        logger.info("lz4 is usually the fastest codec, but it typically has the lowest compression ratio")

    # =========================================================================
    # 6. snappy
    # =========================================================================
    print_header("6. snappy")

    snappy_supported = round_trip_codec(spark, df, out_dir, "snappy", "Read Back: compression=snappy")
    if snappy_supported:
        logger.info("snappy provides a good balance between write speed and compression ratio")

    supported = {
        "none": none_supported,
        "gzip": gzip_supported,
        "bzip2": bzip2_supported,
        "deflate": deflate_supported,
        "lz4": lz4_supported,
        "snappy": snappy_supported,
    }

    summary_file = os.path.join(out_dir, "codec_summary.jsonl")
    write_json_lines(
        summary_file,
        [
            f'{{"codec":"none","tradeoff":"largest output, easiest to inspect","splittable":true,"supported":{str(supported["none"]).lower()}}}',
            f'{{"codec":"gzip","tradeoff":"best compression ratio, slower reads/writes","splittable":false,"supported":{str(supported["gzip"]).lower()}}}',
            f'{{"codec":"bzip2","tradeoff":"good ratio and splittable, but slower","splittable":true,"supported":{str(supported["bzip2"]).lower()}}}',
            f'{{"codec":"deflate","tradeoff":"compact output with moderate speed","splittable":false,"supported":{str(supported["deflate"]).lower()}}}',
            f'{{"codec":"lz4","tradeoff":"fastest throughput, lowest compression ratio","splittable":false,"supported":{str(supported["lz4"]).lower()}}}',
            f'{{"codec":"snappy","tradeoff":"balanced speed and size for general workloads","splittable":false,"supported":{str(supported["snappy"]).lower()}}}',
        ],
    )
    logger.info("Wrote codec comparison summary to %s", summary_file)

    print_success("Read behavior is the same for every section: spark.read.json() auto-detects the codec.")
    print_success(
        "Trade-offs: none=easiest to inspect, gzip=best ratio, bzip2=splittable, lz4=fastest, snappy=best balance."
    )

    unavailable_codecs = [codec for codec, is_supported in supported.items() if not is_supported]
    if unavailable_codecs:
        print_warning(f"Unavailable in this runtime: {', '.join(unavailable_codecs)}")

    spark.stop()
