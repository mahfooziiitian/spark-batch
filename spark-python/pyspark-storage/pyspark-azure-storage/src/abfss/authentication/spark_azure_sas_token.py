import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    account_name = os.environ.get("AZURE_STORAGE_ACCOUNT", "mystorageaccount")
    sas_token = os.environ.get("AZURE_SAS_TOKEN", "")
    container = os.environ.get("AZURE_CONTAINER", "mycontainer")

    hadoop_azure = "3.3.4"
    suffix = f"{account_name}.dfs.core.windows.net"

    spark = (SparkSession.builder
             .appName("azure-read-sas-token")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.jars.packages",
                     f"org.apache.hadoop:hadoop-azure:{hadoop_azure}")
             .config(f"spark.hadoop.fs.azure.sas.token.provider.type.{suffix}",
                     "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
             .config(f"spark.hadoop.fs.azure.sas.fixed.token.{suffix}",
                     sas_token)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    input_path = os.environ.get(
        "INPUT_PATH",
        f"abfss://{container}@{account_name}.dfs.core.windows.net/input.csv")

    df = spark.read.option("header", True).csv(input_path)
    df.show(truncate=False)

    spark.stop()
