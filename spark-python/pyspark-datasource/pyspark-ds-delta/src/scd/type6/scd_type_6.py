"""SCD 6 is an extension of the SCD Type 2 approach. It addresses scenarios where you need to track multiple updates
to the same record within the same update interval. Instead of keeping just two versions (old and new),
SCD 6 maintains a complete history of changes within an update interval.

Combination of Type 2 and Type 3.

"""
import os

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import Window
from pyspark.sql import functions as F

if __name__ == "__main__":
    # Initialize SparkSession
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]
    builder = (
        pyspark.sql.SparkSession.builder.appName("versioning")
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

    # Sample data for the initial dimension table (current state)
    data = [
        (1, "John Doe", "Active"),
        (2, "Jane Smith", "Active"),
        (3, "Bob Johnson", "Active"),
    ]
    columns = ["id", "name", "status"]

    # Create DataFrame for the current state of the dimension table
    current_df = spark.createDataFrame(data, columns)

    # Sample data for the update to the dimension table (new changes)
    update_data = [
        (1, "John Doe", "Inactive"),
        (2, "Jane Smith", "Active"),
        (4, "Alice Brown", "Active"),
    ]
    update_columns = ["id", "name", "status"]

    # Create DataFrame for the update to the dimension table
    update_df = spark.createDataFrame(update_data, update_columns)
    update_df = update_df.withColumn("end_date", F.lit(None))

    # SCD Type 6 implementation
    # Step 1: Add an end_date column to the current_df with a default value (e.g., '9999-12-31')
    current_df = current_df.withColumn("end_date", F.lit("9999-12-31"))

    # Step 2: Create a merged DataFrame that contains both the current and updated data
    merged_df = current_df.union(update_df)

    # Step 3: Use a Window function to partition by 'id' and order by 'end_date' to get the previous row for each ID
    window_spec = Window.partitionBy("id").orderBy(F.col("end_date").desc())
    merged_df = merged_df.withColumn("prev_status", F.lag("status").over(window_spec))

    # Step 4: Update the 'end_date' for the current rows with non-matching statuses between current_df and update_df
    merged_df = merged_df.withColumn(
        "end_date",
        F.when(
            F.col("status") != F.col("prev_status"), F.lit(F.current_date())
        ).otherwise(F.col("end_date")),
    )

    # Step 5: Filter out rows with the same status as the previous status (no changes)
    merged_df = merged_df.filter(F.col("status") != F.col("prev_status"))

    # Step 6: Drop the 'prev_status' column as it's no longer needed
    merged_df = merged_df.drop("prev_status")

    # Show the final merged DataFrame
    merged_df.show()

    # Save the updated dimension data and historical changes to Delta tables
    # For example, you can save the 'merged_df' as the current state and store the changes in a separate Delta table.

    # Write the updated current state to Delta table (current dimension table)
    current_df.write.format("delta").mode("overwrite").saveAsTable(
        "scd.current_dimension_table"
    )

    # Write the historical changes to Delta table (historical changes table)
    historical_changes_df = merged_df.select("id", "name", "status", "end_date")
    historical_changes_df.write.format("delta").mode("append").saveAsTable(
        "scd.historical_changes_table"
    )

    # Stop the SparkSession
    spark.stop()
