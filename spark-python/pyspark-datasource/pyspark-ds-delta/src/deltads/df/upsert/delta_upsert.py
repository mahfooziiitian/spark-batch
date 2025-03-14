import os

import pyspark
from delta import configure_spark_with_delta_pip
from delta.tables import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    builder = (
        pyspark.sql.SparkSession.builder.appName("delta_app")
        # .config('spark.jars.packages', 'io.delta:delta-core_2.13:2.3.0')
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    file_path = (
        "file:///"
        + (os.environ["DATA_HOME"].replace("\\", "/"))
        + "/FileData/Delta/versioning/upsert_table"
    )

    deltaTable = DeltaTable.forPath(spark, file_path)

    # Update every even value by adding 100 to it
    deltaTable.update(condition=expr("id % 2 == 0"), set={"id": expr("id + 100")})

    # Delete every even value
    deltaTable.delete(condition=expr("id % 2 == 0"))

    # Upsert (merge) new data
    newData = spark.range(0, 20)

    deltaTable.alias("oldData").merge(
        newData.alias("newData"), "oldData.id = newData.id"
    ).whenMatchedUpdate(set={"id": col("newData.id")}).whenNotMatchedInsert(
        values={"id": col("newData.id")}
    ).execute()

    deltaTable.toDF().show()
