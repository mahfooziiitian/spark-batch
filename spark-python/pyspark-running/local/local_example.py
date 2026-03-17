"""
PySpark — Local Mode Example
============================
Runs entirely on your machine. No cluster required.

Run:
    python local_example.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# ---------------------------------------------------------------------------
# 1. Create a SparkSession configured for local mode
# ---------------------------------------------------------------------------
spark = (SparkSession.builder
         .appName("local-mode-example")
         .master("local[*]")                          # use all CPU cores
         .config("spark.sql.shuffle.partitions", "4") # reduce for small data
         .config("spark.ui.enabled", "false")         # skip web UI
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

print("Spark version:", spark.version)
print("Master:       ", spark.sparkContext.master)

# ---------------------------------------------------------------------------
# 2. Create sample sales data in-memory
# ---------------------------------------------------------------------------
schema = StructType([
    StructField("order_id",  IntegerType(), False),
    StructField("region",    StringType(),  False),
    StructField("product",   StringType(),  False),
    StructField("quantity",  IntegerType(), False),
    StructField("unit_price",DoubleType(),  False),
])

rows = [
    (1,  "North", "Widget-A", 10, 9.99),
    (2,  "South", "Widget-B", 5,  19.99),
    (3,  "North", "Widget-A", 7,  9.99),
    (4,  "East",  "Widget-C", 12, 4.99),
    (5,  "West",  "Widget-B", 3,  19.99),
    (6,  "East",  "Widget-A", 20, 9.99),
    (7,  "South", "Widget-C", 8,  4.99),
    (8,  "North", "Widget-B", 6,  19.99),
    (9,  "West",  "Widget-A", 15, 9.99),
    (10, "South", "Widget-A", 4,  9.99),
]

orders = spark.createDataFrame(rows, schema)

# ---------------------------------------------------------------------------
# 3. Transform — compute revenue per order
# ---------------------------------------------------------------------------
orders = orders.withColumn("revenue", F.col("quantity") * F.col("unit_price"))

print("\n=== Raw Orders ===")
orders.show()

# ---------------------------------------------------------------------------
# 4. Aggregate — total revenue & units by region
# ---------------------------------------------------------------------------
summary = (orders
           .groupBy("region")
           .agg(
               F.sum("revenue").alias("total_revenue"),
               F.sum("quantity").alias("total_units"),
               F.count("order_id").alias("num_orders"),
           )
           .orderBy(F.desc("total_revenue")))

print("=== Revenue by Region ===")
summary.show()

# ---------------------------------------------------------------------------
# 5. SQL — register a temp view and query it
# ---------------------------------------------------------------------------
orders.createOrReplaceTempView("orders")

top_products = spark.sql("""
    SELECT product,
           SUM(quantity)              AS total_units,
           ROUND(SUM(revenue), 2)     AS total_revenue
    FROM   orders
    GROUP  BY product
    ORDER  BY total_revenue DESC
""")

print("=== Top Products (SQL) ===")
top_products.show()

# ---------------------------------------------------------------------------
# 6. Write output to Parquet (local filesystem)
# ---------------------------------------------------------------------------
output_path = "/tmp/pyspark_local_output"
summary.write.mode("overwrite").parquet(output_path)
print(f"Summary written to: {output_path}")

# Read it back to verify
read_back = spark.read.parquet(output_path)
print("=== Read-back from Parquet ===")
read_back.show()

spark.stop()
print("Done.")
