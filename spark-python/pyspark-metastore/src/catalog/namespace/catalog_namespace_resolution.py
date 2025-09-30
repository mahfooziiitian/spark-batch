from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Three-level namespace in Spark 3+
# Format: catalog.database.table
spark.sql("SELECT * FROM catalog.database.table")

# Two-level namespace: Uses default catalog
spark.sql("SELECT * FROM database.table")

# One-level namespace: Uses default catalog and current database
spark.sql("SELECT * FROM table")

# Set current database in default catalog
spark.sql("USE default")

# Equivalent queries when current database is 'default'
spark.sql("SELECT * FROM my_table")  # Current database
spark.sql("SELECT * FROM default.my_table")  # Explicit database
spark.sql(
    "SELECT * FROM spark_catalog.default.my_table"
)  # Explicit catalog and database

# Example: Switching catalogs (Spark 3+)
spark.sql("USE CATALOG my_catalog")
spark.sql("USE my_database")
spark.sql("SELECT * FROM my_catalog.my_database.my_table")

# List catalogs, databases, and tables
spark.sql("SHOW CATALOGS")
spark.sql("SHOW DATABASES IN my_catalog")
spark.sql("SHOW TABLES IN my_catalog.my_database")

# Create table in a specific catalog and database
spark.sql("""CREATE TABLE my_catalog.my_database.new_table (
        id INT,
        name STRING)
""")
