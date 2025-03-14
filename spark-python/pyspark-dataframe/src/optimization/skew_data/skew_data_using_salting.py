from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, monotonically_increasing_id

if __name__ == '__main__':
    # Create a Spark session
    spark = SparkSession.builder.appName("SkewDataOptimization").getOrCreate()

    # Assume 'df' is your DataFrame with skewed data and 'key_column' is the column with skewed keys
    df = ...

    # Define the number of salt buckets
    num_buckets = 10

    # Add a salt column to the DataFrame
    df_salted = df.withColumn("salt", (rand() * num_buckets).cast("int"))

    # Combine the original key column with the salt column
    df_salted = df_salted.withColumn("salted_key", col("key_column") + col("salt"))

    # Repartition the DataFrame based on the salted key
    df_optimized = df_salted.repartition("salted_key")

    # Drop the salt and salted_key columns if you don't need them anymore
    df_optimized = df_optimized.drop("salt", "salted_key")

    # Perform your transformations on the optimized DataFrame
    # ...

    # Show the optimized DataFrame
    df_optimized.show()

    # Stop the Spark session
    spark.stop()