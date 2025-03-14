import os
import sys

from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == '__main__':
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]
    data_home = os.environ["DATA_HOME"]
    builder = (
        SparkSession.builder.appName("versioning")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.local.dir", f"{data_home}/Processing/Batch/Spark/temp")
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", warehouse_location)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home='{derby_home}'")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Example DataFrame with data
    data = [("John", "New York", 25),
            ("Alice", "San Francisco", 30),
            ("Bob", "Los Angeles", 28),
            ("Eve", "Chicago", 35)]

    columns = ["name", "city", "age"]
    df = spark.createDataFrame(data, columns)

    # Write the DataFrame to a Delta table
    delta_table_path = f"{data_home}/FileData/Delta/crud/delete-table"
    df.write.format("delta").mode("overwrite").save(delta_table_path)

    # Read the Delta table
    delta_table = DeltaTable.forPath(spark, delta_table_path)
    delta_df = delta_table.toDF()
    delta_df.show()

    # Perform delete operation (delete rows where age > 30)
    delta_table.delete(condition="age > 30")

    # Read the Delta table after the delete operation
    delta_df_after_delete = spark.read.format("delta").load(delta_table_path)
    delta_df_after_delete.show()
