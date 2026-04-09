"""Read entire files as single rows using the wholetext option."""
import os
import tempfile

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("read_text_wholetext")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    tmp_dir = tempfile.mkdtemp()
    for name, content in [
        ("poem.txt", "Roses are red\nViolets are blue\nSpark is great\nAnd so are you"),
        ("haiku.txt", "An old silent pond\nA frog jumps into the pond\nSplash! Silence again"),
    ]:
        with open(os.path.join(tmp_dir, name), "w") as f:
            f.write(content)

    # wholetext=false (default) — one row per line
    df_lines = spark.read.text(tmp_dir)
    print("=== Default (line-by-line) ===")
    df_lines.show(truncate=False)
    print(f"Rows: {df_lines.count()}")

    # wholetext=true — one row per file (preserves newlines)
    df_whole = spark.read.option("wholetext", "true").text(tmp_dir)
    print("=== Wholetext (one row per file) ===")
    df_whole.show(truncate=False)
    print(f"Rows: {df_whole.count()}")

    # Wholesale inside text
    df_whole = spark.read.text(tmp_dir, wholetext=True)
    print("=== Wholetext (one row per file) ===")
    df_whole.show(truncate=False)
    print(f"Rows: {df_whole.count()}")


    spark.stop()
