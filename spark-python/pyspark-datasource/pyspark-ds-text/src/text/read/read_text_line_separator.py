"""Use a custom line separator instead of the default newline character."""
import os
import tempfile

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("read_text_line_separator")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- semicolon-separated records in a single line ---
    tmp = os.path.join(tempfile.mkdtemp(), "semicolon.txt")
    with open(tmp, "w") as f:
        f.write("record_one;record_two;record_three;record_four")

    df = spark.read.option("lineSep", ";").text(tmp)
    print("=== Semicolon separator ===")
    df.show(truncate=False)

    # --- pipe-separated records ---
    tmp2 = os.path.join(tempfile.mkdtemp(), "pipe.txt")
    with open(tmp2, "w") as f:
        f.write("alpha|beta|gamma|delta|epsilon")

    df2 = spark.read.option("lineSep", "|").text(tmp2)
    print("=== Pipe separator ===")
    df2.show(truncate=False)

    # --- multi-character separator ---
    tmp3 = os.path.join(tempfile.mkdtemp(), "multi.txt")
    with open(tmp3, "w") as f:
        f.write("first<SEP>second<SEP>third")

    df3 = spark.read.option("lineSep", "<SEP>").text(tmp3)
    print("=== Multi-character separator ===")
    df3.show(truncate=False)

    spark.stop()
