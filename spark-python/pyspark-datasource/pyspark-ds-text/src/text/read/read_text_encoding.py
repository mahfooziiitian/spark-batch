"""Read text files with different character encodings."""
import os
import tempfile

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("read_text_encoding")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    tmp_dir = tempfile.mkdtemp()

    # --- UTF-8 (default) ---
    utf8_path = os.path.join(tmp_dir, "utf8.txt")
    with open(utf8_path, "w", encoding="utf-8") as f:
        f.write("Hello café résumé naïve\nStraße München Zürich\n日本語テスト\n")

    print("=== UTF-8 (default encoding) ===")
    spark.read.text(utf8_path).show(truncate=False)

    # --- Latin-1 / ISO-8859-1 ---
    latin1_path = os.path.join(tmp_dir, "latin1.txt")
    with open(latin1_path, "w", encoding="iso-8859-1") as f:
        f.write("café résumé naïve\nStraße München Zürich\n")

    print("=== ISO-8859-1 with explicit encoding ===")
    spark.read.option("encoding", "ISO-8859-1").text(latin1_path).show(truncate=False)

    # --- UTF-16 ---
    utf16_path = os.path.join(tmp_dir, "utf16.txt")
    with open(utf16_path, "w", encoding="utf-16") as f:
        f.write("UTF-16 encoded text\nSecond line\n")

    print("=== UTF-16 ===")
    spark.read.option("encoding", "UTF-16").text(utf16_path).show(truncate=False)

    spark.stop()
