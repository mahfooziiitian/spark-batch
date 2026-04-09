"""Filter which files to read using pathGlobFilter — useful for mixed-format directories."""
import os
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("read_text_path_glob_filter")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    base = tempfile.mkdtemp()
    files = {
        "access.log": "GET /index.html 200\nPOST /api/data 201",
        "error.log": "ERROR NullPointerException\nERROR OutOfMemory",
        "readme.md": "# This is not a log file",
        "config.yaml": "key: value",
    }
    for name, content in files.items():
        with open(os.path.join(base, name), "w") as f:
            f.write(content)

    # read only .log files from the directory
    print("=== pathGlobFilter *.log ===")
    df = (
        spark.read.option("pathGlobFilter", "*.log")
        .text(base)
        .withColumn("file", F.input_file_name())
    )
    df.show(truncate=False)

    # combine with recursiveFileLookup for nested dirs
    nested = os.path.join(base, "sub")
    os.makedirs(nested)
    with open(os.path.join(nested, "debug.log"), "w") as f:
        f.write("DEBUG connection pool initialized")
    with open(os.path.join(nested, "notes.txt"), "w") as f:
        f.write("some notes")

    print("=== Recursive + pathGlobFilter *.log ===")
    df2 = (
        spark.read.option("pathGlobFilter", "*.log")
        .option("recursiveFileLookup", "true")
        .text(base)
        .withColumn("file", F.input_file_name())
    )
    df2.show(truncate=False)

    spark.stop()
