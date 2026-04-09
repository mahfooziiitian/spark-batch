"""Parse text lines into structured columns using split, substring, and casting."""
import os
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("parse_text_columns")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    tmp = os.path.join(tempfile.mkdtemp(), "products.txt")
    with open(tmp, "w") as f:
        f.write("1|Laptop|999.99|Electronics\n")
        f.write("2|Mouse|29.50|Accessories\n")
        f.write("3|Keyboard|79.00|Accessories\n")
        f.write("4|Monitor|349.99|Electronics\n")
        f.write("5|Webcam|59.95|Accessories\n")

    df_raw = spark.read.text(tmp)
    print("=== Raw text ===")
    df_raw.show(truncate=False)

    # --- split by delimiter into columns ---
    print("=== Split into columns ===")
    df_parsed = (
        df_raw.withColumn("parts", F.split(F.col("value"), "\\|"))
        .select(
            F.col("parts")[0].cast(IntegerType()).alias("id"),
            F.col("parts")[1].alias("name"),
            F.col("parts")[2].cast(DoubleType()).alias("price"),
            F.col("parts")[3].alias("category"),
        )
    )
    df_parsed.printSchema()
    df_parsed.show()

    # --- fixed-width parsing using substring ---
    tmp2 = os.path.join(tempfile.mkdtemp(), "fixed_width.txt")
    with open(tmp2, "w") as f:
        # cols: id(5) name(15) dept(11)
        f.write("00001John Smith     Engineering\n")
        f.write("00002Jane Doe       Marketing  \n")
        f.write("00003Bob Johnson    Sales      \n")

    print("=== Fixed-width parsing ===")
    df_fixed = spark.read.text(tmp2)
    df_fixed = df_fixed.select(
        F.trim(F.substring(F.col("value"), 1, 5)).cast(IntegerType()).alias("id"),
        F.trim(F.substring(F.col("value"), 6, 15)).alias("name"),
        F.trim(F.substring(F.col("value"), 21, 11)).alias("department"),
    )
    df_fixed.show()

    # --- key=value parsing ---
    tmp3 = os.path.join(tempfile.mkdtemp(), "kvpairs.txt")
    with open(tmp3, "w") as f:
        f.write("host=server01 cpu=72.5 mem=8192\n")
        f.write("host=server02 cpu=45.1 mem=16384\n")
        f.write("host=server03 cpu=91.3 mem=4096\n")

    print("=== Key=Value parsing ===")
    df_kv = spark.read.text(tmp3)
    df_kv = df_kv.select(
        F.regexp_extract("value", r"host=(\S+)", 1).alias("host"),
        F.regexp_extract("value", r"cpu=(\S+)", 1).cast(DoubleType()).alias("cpu"),
        F.regexp_extract("value", r"mem=(\S+)", 1).cast(IntegerType()).alias("mem_mb"),
    )
    df_kv.show()

    spark.stop()
