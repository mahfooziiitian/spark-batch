"""SQL access — query a custom data source through a temporary view.

Key concepts:
    - Any custom DataSource-backed DataFrame can be exposed via createOrReplaceTempView
    - Downstream consumers can then use plain Spark SQL, unaware of the custom connector
"""

from __future__ import annotations

from custom_ds import SimpleDataSource, create_spark_session

if __name__ == "__main__":
    spark = create_spark_session("simple-sql")

    spark.dataSource.register(SimpleDataSource)

    spark.read.format("simple").option("numRows", 15).load().createOrReplaceTempView("simple_rows")

    spark.sql("SELECT * FROM simple_rows WHERE id % 2 = 0 ORDER BY id").show()

    spark.stop()
