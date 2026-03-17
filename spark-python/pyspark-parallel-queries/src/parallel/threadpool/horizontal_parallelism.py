"""
Pattern: ThreadPool — horizontal (file/partition) parallelism
=============================================================
Splits an input path into N file chunks and processes each chunk in a
separate thread.  Equivalent to a fan-out / map-reduce using Spark jobs.

Compared to simply reading all files in one job, this gives independent
Spark job groups per chunk, enabling per-chunk cancellation, separate
scheduler pools, and isolated failure domains.

Environment variables
---------------------
DATA_HOME    Root directory for input files.  Falls back to in-memory data.
NUM_CHUNKS   How many parallel chunks.  Default: 4
OUTPUT_PATH  Parquet output directory.  Default: /tmp/horizontal_parallelism
"""

import os
import time
from multiprocessing.pool import ThreadPool
from pathlib import Path
from threading import Lock, current_thread
from typing import NamedTuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

DATA_HOME   = os.environ.get("DATA_HOME", "")
NUM_CHUNKS  = int(os.environ.get("NUM_CHUNKS", "4"))
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/horizontal_parallelism")

# In-memory sample data simulating several input files
_SAMPLE_ROWS = [
    (i, f"user_{i % 50}", round(100 + i * 1.5, 2), f"2024-01-{(i % 28) + 1:02d}")
    for i in range(1, 201)
]
_SCHEMA = ["record_id", "user", "amount", "date"]


class ChunkResult(NamedTuple):
    chunk_id: int
    rows:     int
    revenue:  float
    elapsed:  float


_results: list[ChunkResult] = []
_lock = Lock()


def _process_chunk(chunk_id: int, df: DataFrame) -> None:
    t = current_thread().name
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
    spark.sparkContext.setJobDescription(f"chunk:{chunk_id}")

    start = time.perf_counter()
    agg = df.agg(F.count("*").alias("rows"),
                 F.round(F.sum("amount"), 2).alias("revenue")).first()
    elapsed = time.perf_counter() - start

    cr = ChunkResult(chunk_id, agg["rows"], agg["revenue"], round(elapsed, 3))
    with _lock:
        _results.append(cr)
    print(f"  [{t}] chunk {chunk_id}: {agg['rows']:,} rows, revenue={agg['revenue']:,.2f} ({elapsed:.3f}s)")


def _split_df(df: DataFrame, n: int) -> list[DataFrame]:
    """Split DataFrame into n roughly equal sub-DataFrames using row ranges."""
    total = df.count()
    chunk_size = max(1, total // n)
    df_indexed = df.withColumn("_idx", F.monotonically_increasing_id())
    chunks = []
    for i in range(n):
        lo, hi = i * chunk_size, (i + 1) * chunk_size
        if i == n - 1:
            hi = total + 1
        chunks.append(df_indexed.filter((F.col("_idx") >= lo) & (F.col("_idx") < hi))
                                 .drop("_idx"))
    return chunks


def _load_df(spark: SparkSession) -> DataFrame:
    if DATA_HOME and os.path.isdir(DATA_HOME):
        return spark.read.parquet(DATA_HOME)
    return spark.createDataFrame(_SAMPLE_ROWS, _SCHEMA)


def run_parallel(spark: SparkSession) -> tuple[list[ChunkResult], float]:
    _results.clear()
    df = _load_df(spark).cache()
    df.count()
    chunks = _split_df(df, NUM_CHUNKS)
    start = time.perf_counter()
    with ThreadPool(NUM_CHUNKS) as pool:
        pool.starmap(_process_chunk, enumerate(chunks))
    return list(_results), time.perf_counter() - start


def run_serial(spark: SparkSession) -> tuple[list[ChunkResult], float]:
    _results.clear()
    df = _load_df(spark).cache()
    df.count()
    chunks = _split_df(df, NUM_CHUNKS)
    start = time.perf_counter()
    for chunk_id, chunk_df in enumerate(chunks):
        _process_chunk(chunk_id, chunk_df)
    return list(_results), time.perf_counter() - start


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from utils.spark_session import get_spark

    if not DATA_HOME:
        print("DATA_HOME not set — using in-memory sample data\n")

    spark = get_spark("threadpool-horizontal-parallelism")
    try:
        print(f"Chunks: {NUM_CHUNKS}\n")

        print("── Serial run ──────────────────────────────────")
        _, serial_secs = run_serial(spark)

        print("\n── Parallel run ────────────────────────────────")
        results, parallel_secs = run_parallel(spark)

        print("\n── Per-chunk summary ───────────────────────────")
        print(f"  {'Chunk':>6} {'Rows':>8} {'Revenue':>14} {'Time (s)':>10}")
        print("  " + "-" * 42)
        for r in sorted(results, key=lambda x: x.chunk_id):
            print(f"  {r.chunk_id:>6} {r.rows:>8,} {r.revenue:>14,.2f} {r.elapsed:>10.3f}")

        total_rev = sum(r.revenue for r in results)
        total_rows = sum(r.rows for r in results)
        print("  " + "-" * 42)
        print(f"  {'TOTAL':>6} {total_rows:>8,} {total_rev:>14,.2f}")

        print(f"\nSerial   : {serial_secs:.2f}s")
        print(f"Parallel : {parallel_secs:.2f}s")
        print(f"Speedup  : {serial_secs / parallel_secs:.2f}x")
    finally:
        spark.stop()
