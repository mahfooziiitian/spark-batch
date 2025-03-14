"""
Sometimes a job that writes data to a Delta table is restarted due to various reasons (for example, job encounters a failure). The failed job may or may not have written the data to Delta table before terminating. In the case where the data is written to the Delta table, the restarted job writes the same data to the Delta table which results in duplicate data.

To address this, Delta tables support the following DataFrameWriter options to make the writes idempotent:

    txnAppId: A unique string that you can pass on each DataFrame write. For example, this can be the name of the job.

    txnVersion: A monotonically increasing number that acts as transaction version. This number needs to be unique for data that is being written to the Delta table(s). For example, this can be the epoch seconds of the instant when the query is attempted for the first time. Any subsequent restarts of the same job needs to have the same value for txnVersion.

spark.databricks.delta.write.txnAppId
spark.databricks.delta.write.txnVersion

spark.databricks.delta.write.txnVersion.autoReset.enabled to true to automatically reset spark.databricks.delta.write.txnVersion.

This solution assumes that the data being written to Delta table(s) in multiple retries of the job is same. If a write attempt in a Delta table succeeds but due to some downstream failure there is a second write attempt with same txn options but different data, then that second write attempt will be ignored. This can cause unexpected results.

"""
import os
import sys

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip, DeltaTable

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

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

    app_id = ""  # A unique string that is used as an application ID.
    version = ""  # A monotonically increasing number that acts as transaction version.

    df = spark.read.format("delta").load("")

    (df.write.format("delta").option("txnVersion", version)
     .option("txnAppId", app_id).save("table"))
