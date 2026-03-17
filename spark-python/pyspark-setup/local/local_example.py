"""
PySpark — Local Mode Setup Example
====================================
Verifies that PySpark is correctly installed and configured for local development.
Creates a SparkSession, runs a small ETL pipeline, and writes output to Parquet.

Run:
    python local/local_example.py
    # or:
    spark-submit --master local[*] local/local_example.py
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# ── 1. SparkSession ────────────────────────────────────────────────────────────
spark = (SparkSession.builder
         .appName("local-setup-example")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "4")   # keep low for local dev
         .config("spark.ui.enabled", "false")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("PySpark Local Mode — Setup Verification")
print("=" * 60)
print(f"  Spark version   : {spark.version}")
print(f"  Python version  : {sys.version.split()[0]}")
print(f"  Master          : {spark.sparkContext.master}")
print(f"  App name        : {spark.sparkContext.appName}")
print()

# ── 2. Sample data — customer orders ──────────────────────────────────────────
schema = StructType([
    StructField("customer_id", StringType(),  False),
    StructField("name",        StringType(),  False),
    StructField("region",      StringType(),  False),
    StructField("product",     StringType(),  False),
    StructField("quantity",    IntegerType(), False),
    StructField("unit_price",  DoubleType(),  False),
])

rows = [
    ("C001", "Alice", "North", "Laptop",     2, 999.99),
    ("C002", "Bob",   "South", "Phone",      1, 699.49),
    ("C001", "Alice", "North", "Tablet",     3, 349.00),
    ("C003", "Carol", "East",  "Laptop",     1, 999.99),
    ("C004", "Dave",  "West",  "Phone",      2, 699.49),
    ("C002", "Bob",   "South", "Headphones", 4, 129.99),
    ("C003", "Carol", "East",  "Tablet",     2, 349.00),
    ("C005", "Eve",   "North", "Laptop",     1, 999.99),
    ("C004", "Dave",  "West",  "Headphones", 3, 129.99),
    ("C005", "Eve",   "North", "Phone",      1, 699.49),
]

orders = spark.createDataFrame(rows, schema)
orders = orders.withColumn("revenue", F.round(F.col("quantity") * F.col("unit_price"), 2))

print("=== Orders ===")
orders.show()

# ── 3. Aggregation — revenue by region ────────────────────────────────────────
by_region = (orders
             .groupBy("region")
             .agg(
                 F.round(F.sum("revenue"), 2).alias("total_revenue"),
                 F.sum("quantity").alias("total_units"),
                 F.countDistinct("customer_id").alias("unique_customers"),
             )
             .orderBy(F.desc("total_revenue")))

print("=== Revenue by Region ===")
by_region.show()

# ── 4. Top products via SQL ────────────────────────────────────────────────────
orders.createOrReplaceTempView("orders")

top_products = spark.sql("""
    SELECT  product,
            SUM(quantity)               AS total_units,
            ROUND(SUM(revenue), 2)      AS total_revenue,
            COUNT(DISTINCT customer_id) AS buyers
    FROM    orders
    GROUP   BY product
    ORDER   BY total_revenue DESC
""")

print("=== Top Products (SQL) ===")
top_products.show()

# ── 5. Write to Parquet and read back ─────────────────────────────────────────
output_path = "/tmp/pyspark_setup_local"
by_region.write.mode("overwrite").parquet(output_path)

read_back = spark.read.parquet(output_path)
print(f"=== Read back from {output_path} ===")
read_back.show()

spark.stop()
print("Local setup verification complete.")
