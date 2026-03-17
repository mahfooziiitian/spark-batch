"""
PySpark Script — Sales ETL Pipeline
=====================================
A realistic multi-step ETL job suitable for any Spark cluster.

Steps:
  1. Ingest raw CSV-like data (in-memory for demo; replace with real paths)
  2. Clean & validate
  3. Enrich with a product dimension
  4. Aggregate KPIs by region and product category
  5. Write Parquet output partitioned by region

Submit locally:
    spark-submit --master local[*] sales_analysis.py

Submit to YARN:
    spark-submit --master yarn --deploy-mode cluster \
        --num-executors 4 --executor-memory 4g \
        sales_analysis.py

Environment variables (optional overrides):
    INPUT_PATH   — path to raw orders data
    DIM_PATH     — path to product dimension table
    OUTPUT_PATH  — where to write aggregated output
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField,
                                IntegerType, StringType, DoubleType, DateType)

# ---------------------------------------------------------------------------
# 1. SparkSession
# ---------------------------------------------------------------------------
spark = (SparkSession.builder
         .appName("sales-etl-pipeline")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

print(f"Running on master: {spark.sparkContext.master}")

# ---------------------------------------------------------------------------
# 2. Ingest — raw orders  (replace createDataFrame with spark.read.csv in prod)
# ---------------------------------------------------------------------------
INPUT_PATH = os.environ.get("INPUT_PATH")

orders_schema = StructType([
    StructField("order_id",    IntegerType(), False),
    StructField("order_date",  StringType(),  False),
    StructField("region",      StringType(),  True),
    StructField("product_id",  StringType(),  False),
    StructField("quantity",    IntegerType(),  True),
    StructField("unit_price",  DoubleType(),   True),
    StructField("discount_pct",DoubleType(),   True),
])

if INPUT_PATH:
    raw_orders = spark.read.schema(orders_schema).csv(INPUT_PATH, header=True)
else:
    raw_rows = [
        (1001, "2024-01-05", "North",  "P001", 10, 9.99,  0.0),
        (1002, "2024-01-06", "South",  "P002", 5,  19.99, 5.0),
        (1003, "2024-01-07", "North",  "P001", 7,  9.99,  0.0),
        (1004, "2024-01-08", "East",   "P003", 12, 4.99,  10.0),
        (1005, "2024-01-09", "West",   "P002", 3,  19.99, 0.0),
        (1006, "2024-01-10", "East",   "P001", 20, 9.99,  5.0),
        (1007, "2024-01-11", "South",  "P003", 8,  4.99,  0.0),
        (1008, "2024-01-12", "North",  "P002", 6,  19.99, 10.0),
        (1009, "2024-01-13", "West",   "P001", 15, 9.99,  0.0),
        (1010, "2024-01-14", None,     "P001", 4,  9.99,  0.0),   # bad row
        (1011, "2024-01-15", "North",  "P004", -2, 14.99, 0.0),   # bad qty
    ]
    raw_orders = spark.createDataFrame(raw_rows, orders_schema)

print(f"Raw orders count: {raw_orders.count()}")

# ---------------------------------------------------------------------------
# 3. Product dimension
# ---------------------------------------------------------------------------
DIM_PATH = os.environ.get("DIM_PATH")

product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
])

if DIM_PATH:
    products = spark.read.schema(product_schema).csv(DIM_PATH, header=True)
else:
    product_rows = [
        ("P001", "Widget-A", "Widgets"),
        ("P002", "Widget-B", "Widgets"),
        ("P003", "Gadget-X", "Gadgets"),
    ]
    products = spark.createDataFrame(product_rows, product_schema)

# ---------------------------------------------------------------------------
# 4. Clean & validate
# ---------------------------------------------------------------------------
cleaned = (raw_orders
           .filter(F.col("region").isNotNull())        # drop rows with no region
           .filter(F.col("quantity") > 0)              # drop negative/zero qty
           .filter(F.col("unit_price") > 0)
           .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
           .withColumn("discount_pct",
                       F.coalesce(F.col("discount_pct"), F.lit(0.0))))

print(f"Cleaned orders count: {cleaned.count()}")

# ---------------------------------------------------------------------------
# 5. Enrich — join with product dimension
#    Left join keeps orders even if product is unknown (flagged as "Unknown")
# ---------------------------------------------------------------------------
enriched = (cleaned
            .join(products, on="product_id", how="left")
            .withColumn("category",
                        F.coalesce(F.col("category"), F.lit("Unknown")))
            .withColumn("product_name",
                        F.coalesce(F.col("product_name"), F.col("product_id"))))

# ---------------------------------------------------------------------------
# 6. Compute KPIs
# ---------------------------------------------------------------------------
kpi = enriched.withColumn(
    "net_revenue",
    F.round(
        F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100),
        2,
    )
)

# ---------------------------------------------------------------------------
# 7. Aggregate by region + category
# ---------------------------------------------------------------------------
agg = (kpi
       .groupBy("region", "category")
       .agg(
           F.count("order_id").alias("num_orders"),
           F.sum("quantity").alias("total_units"),
           F.round(F.sum("net_revenue"), 2).alias("total_revenue"),
           F.round(F.avg("net_revenue"), 2).alias("avg_order_revenue"),
       )
       .orderBy("region", F.desc("total_revenue")))

print("\n=== KPIs by Region & Category ===")
agg.show()

# ---------------------------------------------------------------------------
# 8. Monthly trend
# ---------------------------------------------------------------------------
monthly = (kpi
           .withColumn("month", F.date_format("order_date", "yyyy-MM"))
           .groupBy("month")
           .agg(F.round(F.sum("net_revenue"), 2).alias("monthly_revenue"))
           .orderBy("month"))

print("=== Monthly Revenue Trend ===")
monthly.show()

# ---------------------------------------------------------------------------
# 9. Write output partitioned by region
# ---------------------------------------------------------------------------
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/sales_etl_output")

(agg
 .write
 .mode("overwrite")
 .partitionBy("region")
 .parquet(OUTPUT_PATH))

print(f"Output written to: {OUTPUT_PATH}")

spark.stop()
print("Pipeline complete.")
