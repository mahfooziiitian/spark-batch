import os

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("print-all-config")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # --- Method 1: SparkContext getConf ---
    print("=" * 60)
    print("SparkContext Configuration (getConf)")
    print("=" * 60)
    for key, value in sorted(spark.sparkContext.getConf().getAll()):
        print(f"  {key} = {value}")

    # --- Method 2: Spark SQL SET -v ---
    print()
    print("=" * 60)
    print("Spark SQL Configuration (SET -v)")
    print("=" * 60)
    sql_configs = spark.sql("SET -v").collect()
    for row in sql_configs[:20]:
        print(f"  {row.key} = {row.value}")
    print(f"  ... ({len(sql_configs)} total keys)")

    # --- Method 3: Query a single key ---
    print()
    print("=" * 60)
    print("Single Key Lookup")
    print("=" * 60)
    print("  spark.app.name          =", spark.conf.get("spark.app.name"))
    print("  spark.sql.adaptive      =", spark.conf.get("spark.sql.adaptive.enabled"))
    print("  shuffle.partitions      =", spark.conf.get("spark.sql.shuffle.partitions"))

    spark.stop()
