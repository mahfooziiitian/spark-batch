import os
import sys
from pathlib import Path

import pyspark
from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["DERBY_HOME"]
    builder = (
        pyspark.sql.SparkSession.builder.appName("replace_column")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", warehouse_location)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home='{derby_home}'")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    data_home = Path(os.environ["DATA_HOME"])
    delta_table_path = data_home / "data" / "delta" / "schema" / "replace_column"
    delta_table_path.mkdir(parents=True, exist_ok=True)
    table_path = str(delta_table_path)

    # Create an initial Delta table
    print("Creating initial Delta table...")
    initial_data = [("Alice", 30, "New York"), ("Bob", 24, "London")]
    initial_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("city", StringType(), True),
        ]
    )
    initial_df = spark.createDataFrame(initial_data, initial_schema)
    initial_df.write.format("delta").mode("overwrite").save(table_path)
    print("Initial table content:")
    spark.read.format("delta").load(table_path).show()

    # Example 1: Replace a column with a new data type (requires overwriteSchema)
    print("\nExample 1: Replacing 'age' column with String")
    delta_table = DeltaTable.forPath(spark, table_path)
    delta_table.update(condition="age IS NOT NULL", set={"age": "CAST(age AS STRING)"})
    delta_table = DeltaTable.forPath(spark, table_path)
