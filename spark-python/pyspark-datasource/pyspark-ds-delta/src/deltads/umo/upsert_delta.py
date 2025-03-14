import os

import pyspark
from delta import DeltaTable, configure_spark_with_delta_pip

if __name__ == "__main__":
    # Create a SparkSession
    builder = (
        pyspark.sql.SparkSession.builder.appName("upsert_delta")
        .config("spark.jars.packages", "io.delta:delta-core_2.13:2.3.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    table_path = (
        "file:///"
        + (os.environ["DATA_HOME"].replace("\\", "/"))
        + "/FileData/Delta/upsert_delta"
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    old_df = spark.createDataFrame(
        [(1, "John", "Jersy"), (2, "Alam", "Chicago")], ["id", "name", "city"]
    )

    old_df.write.format("delta").mode("overwrite").save(table_path)

    # Load an existing Delta table
    delta_table = DeltaTable.forPath(spark, table_path)

    # Define the new data with updated values
    new_data = spark.createDataFrame(
        [(1, "John", "New York"), (2, "Jane", "Chicago"), (3, "Alice", "Seattle")],
        ["id", "name", "city"],
    )

    # Retrieve the schema of the Delta table
    delta_schema = delta_table.toDF().schema

    # Get the columns that have changed in the new data compared to the existing data
    changed_columns = [
        column for column in delta_schema.names if column in new_data.columns
    ]

    # Update only the changed columns dynamically
    (
        delta_table.alias("old")
        .merge(new_data.alias("new"), "old.id = new.id")
        .whenMatchedUpdate(set={column: "new." + column for column in changed_columns})
        .whenNotMatchedInsert(
            values={column: "new." + column for column in delta_schema.names}
        )
        .execute()
    )

    # Example: Query the Delta table
    result = spark.sql(f"SELECT * FROM delta.`{table_path}`")
    result.show()
