"""
Pattern: threading.Thread
=========================
Run two independent Spark actions concurrently by wrapping each in a Python
thread.  Both threads share the same ``SparkSession`` (thread-safe) and a
single cached DataFrame.  The FAIR scheduler interleaves their tasks across
executor cores so neither job starves the other.

What this example demonstrates
-------------------------------
- Caching a shared DataFrame before spawning threads
- Assigning each thread to a named FAIR scheduler pool
- Using ``threading.Lock`` to protect the shared ``results`` dict
- Comparing serial vs parallel wall-clock time

Environment variables
---------------------
INPUT_PATH   Path to a plain-text file (one sentence per line).
             Falls back to the built-in Shakespeare sample.
OUTPUT_WORD  Parquet output for word counts.  Default: /tmp/word_char/wordcount
OUTPUT_CHAR  Parquet output for char counts.  Default: /tmp/word_char/charcount
"""

import os
import threading
import time
from threading import Lock

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

INPUT_PATH  = os.environ.get("INPUT_PATH", "")
OUTPUT_WORD = os.environ.get("OUTPUT_WORD", "/tmp/word_char/wordcount")
OUTPUT_CHAR = os.environ.get("OUTPUT_CHAR", "/tmp/word_char/charcount")

SAMPLE_TEXT = [
    "to be or not to be that is the question",
    "all the world is a stage and all the men and women merely players",
    "the quality of mercy is not strained it droppeth as the gentle rain from heaven",
    "we know what we are but know not what we may be",
    "all that glitters is not gold often have you heard that told",
    "brevity is the soul of wit",
    "the course of true love never did run smooth",
    "what is in a name that which we call a rose by any other name would smell as sweet",
]


def build_words_df(spark: SparkSession) -> DataFrame:
    """Load text input and explode into individual word rows (cached)."""
    if INPUT_PATH and os.path.exists(INPUT_PATH):
        raw = spark.read.text(INPUT_PATH)
    else:
        raw = spark.createDataFrame([(line,) for line in SAMPLE_TEXT], ["value"])

    return (
        raw
        .select(F.explode(F.split(F.col("value"), r"\s+")).alias("word"))
        .filter(F.col("word") != "")
        .cache()
    )


def word_count(df: DataFrame, output_path: str, results: dict, lock: Lock) -> None:
    """Count occurrences of each word and write to Parquet."""
    name = threading.current_thread().name
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")

    start = time.perf_counter()
    wc = (
        df.groupBy("word")
          .agg(F.count("*").alias("count"))
          .orderBy(F.desc("count"))
    )
    wc.write.mode("overwrite").parquet(output_path)
    elapsed = time.perf_counter() - start

    with lock:
        results["word_count"] = {
            "rows": wc.count(),
            "secs": elapsed,
            "thread": name,
        }
    first_row = wc.first()
    top_word = first_row["word"] if first_row else "N/A"
    print(f"[{name}] word_count done in {elapsed:.2f}s — top word: {top_word!r}")


def char_count(df: DataFrame, output_path: str, results: dict, lock: Lock) -> None:
    """Count occurrences of each character and write to Parquet."""
    name = threading.current_thread().name
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")

    start = time.perf_counter()
    cc = (
        df
        .select(F.explode(F.split(F.col("word"), "")).alias("char"))
        .filter(F.col("char") != "")
        .groupBy("char")
        .agg(F.count("*").alias("count"))
        .orderBy(F.desc("count"))
    )
    cc.write.mode("overwrite").parquet(output_path)
    elapsed = time.perf_counter() - start

    with lock:
        results["char_count"] = {
            "rows": cc.count(),
            "secs": elapsed,
            "thread": name,
        }
    first_row = cc.first()
    top_char = first_row["char"] if first_row else "N/A"
    print(f"[{name}] char_count done in {elapsed:.2f}s — top char: {top_char!r}")


def run_serial(df: DataFrame) -> tuple[dict, float]:
    """Run word_count and char_count sequentially (baseline)."""
    results: dict = {}
    lock = Lock()
    start = time.perf_counter()
    word_count(df, OUTPUT_WORD + "_serial", results, lock)
    char_count(df, OUTPUT_CHAR + "_serial", results, lock)
    return results, time.perf_counter() - start


def run_parallel(df: DataFrame) -> tuple[dict, float]:
    """Run word_count and char_count concurrently on two threads."""
    results: dict = {}
    lock = Lock()
    t1 = threading.Thread(
        target=word_count,
        args=(df, OUTPUT_WORD + "_parallel", results, lock),
        name="WordCount",
    )
    t2 = threading.Thread(
        target=char_count,
        args=(df, OUTPUT_CHAR + "_parallel", results, lock),
        name="CharCount",
    )

    start = time.perf_counter()
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    return results, time.perf_counter() - start


if __name__ == "__main__":
    from parallel.utils.spark_session import get_spark

    spark = get_spark("thread-word-char-count")
    try:
        words_df = build_words_df(spark)
        total_words = words_df.count()
        print(f"\nDataset: {total_words:,} words from {len(SAMPLE_TEXT)} lines\n")

        print("── Serial run ──────────────────────────")
        _, serial_secs = run_serial(words_df)

        print("\n── Parallel run ────────────────────────")
        _, parallel_secs = run_parallel(words_df)

        print(f"\n{'Serial':<12}: {serial_secs:.2f}s")
        print(f"{'Parallel':<12}: {parallel_secs:.2f}s")
        print(f"{'Speedup':<12}: {serial_secs / parallel_secs:.2f}x")
    finally:
        spark.stop()
