import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    account_name = os.environ.get("AZURE_STORAGE_ACCOUNT", "mystorageaccount")
    account_key = os.environ.get("AZURE_STORAGE_KEY", "")
    container = os.environ.get("AZURE_CONTAINER", "mycontainer")

    hadoop_azure = "3.3.4"
    spark = (SparkSession.builder
             .appName("azure-read-account-key")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.jars.packages",
                     f"org.apache.hadoop:hadoop-azure:{hadoop_azure}")
             .config(f"spark.hadoop.fs.azure.account.key."
                     f"{account_name}.dfs.core.windows.net",
                     account_key)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    input_path = os.environ.get(
        "INPUT_PATH",
        f"abfss://{container}@{account_name}.dfs.core.windows.net/input.csv")
    output_path = os.environ.get(
        "OUTPUT_PATH",
        f"abfss://{container}@{account_name}.dfs.core.windows.net/output")

    df = spark.read.option("header", True).csv(input_path)
    df.show(truncate=False)

    df.write.mode("overwrite").parquet(output_path)

    spark.stop()
