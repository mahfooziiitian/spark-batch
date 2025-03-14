"""
Delta Lake uses the following rules to determine whether a write from a DataFrame to a table is compatible:

    All DataFrame columns must exist in the target table. If there are columns in the DataFrame not present in the
    table, an exception is raised. Columns present in the table but not in the DataFrame are set to null.

    DataFrame column data types must match the column data types in the target table. If they don’t match,
    an exception is raised.

    DataFrame column names cannot differ only by case. This means that you cannot have columns such as "Foo" and
    “foo” defined in the same table. While you can use Spark in case sensitive or insensitive (default) mode,
    Parquet is case sensitive when storing and returning column information. Delta Lake is case-preserving but
    insensitive when storing the schema and has this restriction to avoid potential mistakes, data corruption,
    or loss issues.

Delta Lake support DDL to add new columns explicitly and the ability to update schema automatically.
"""
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

if __name__ == '__main__':
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]
    data_home = os.environ["DATA_HOME"]
    builder = (
        SparkSession.builder.appName("versioning")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.local.dir", f"{data_home}/Processing/Batch/Spark/temp")
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", warehouse_location)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home='{derby_home}'")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
