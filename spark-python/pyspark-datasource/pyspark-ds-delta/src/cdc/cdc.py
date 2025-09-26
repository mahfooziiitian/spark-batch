from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit

# Initialize SparkSession
spark = (
    SparkSession.builder.appName("DeltaCDC")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

# Define a path for the Delta table
delta_table_path = "/tmp/delta_cdc_table"

# 1. Initial Data Load
print("--- Initial Data Load ---")
data = [(1, "Alice", 30, "NY"), (2, "Bob", 24, "CA"), (3, "Charlie", 35, "TX")]
columns = ["id", "name", "age", "state"]
df = spark.createDataFrame(data, columns)

df.write.format("delta").mode("overwrite").save(delta_table_path)
print("Initial table content:")
DeltaTable.forPath(spark, delta_table_path).toDF().show()

# 2. Insert New Records
print("\n--- Inserting New Records ---")
new_data = [(4, "David", 28, "FL"), (5, "Eve", 22, "WA")]
new_df = spark.createDataFrame(new_data, columns)
new_df.write.format("delta").mode("append").save(delta_table_path)
print("Table after inserts:")
DeltaTable.forPath(spark, delta_table_path).toDF().show()

# 3. Update Existing Records
print("\n--- Updating Records ---")
deltaTable = DeltaTable.forPath(spark, delta_table_path)
deltaTable.update(condition=expr("id = 1"), set={"age": lit(31), "state": lit("NJ")})
print("Table after update:")
DeltaTable.forPath(spark, delta_table_path).toDF().show()
