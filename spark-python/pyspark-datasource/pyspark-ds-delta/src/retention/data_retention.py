"""
To time travel to a previous version, you must retain both the log and the data files for that version.
The data files backing a Delta table are never deleted automatically; data files are deleted only when you run VACUUM.
VACUUM does not delete Delta log files; log files are automatically cleaned up after checkpoints are written.
By default, you can time travel to a Delta table up to 30 days old unless you have:
Run VACUUM on your Delta table.

Changed the data or log file retention periods using the following table properties:

    delta.logRetentionDuration = "interval <interval>": controls how long the history for a table is kept. The
    default is interval 30 days.

    Each time a checkpoint is written, Delta automatically cleans up log entries older than the retention interval.
    If you set this config to a large enough value, many log entries are retained. This should not impact performance
    as operations against the log are constant time. Operations on history are parallel but will become more
    expensive as the log size increases.

    delta.deletedFileRetentionDuration = "interval <interval>": controls how long ago a file must have been deleted
    before being a candidate for VACUUM. The default is interval 7 days.

"""
import os

from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession

if __name__ == '__main__':
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]
    data_home = os.environ["DATA_HOME"]
    builder = (
        SparkSession.builder.appName("data_retention")
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
    data_path = f"{data_home}/FileData/Delta/versioning/upsert_table"

    DeltaTable.forPath(spark, data_path).detail().show(truncate=False)
