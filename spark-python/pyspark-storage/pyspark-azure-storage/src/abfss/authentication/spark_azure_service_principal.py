import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    account_name = os.environ.get("AZURE_STORAGE_ACCOUNT", "mystorageaccount")
    client_id = os.environ.get("AZURE_CLIENT_ID", "")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET", "")
    tenant_id = os.environ.get("AZURE_TENANT_ID", "")
    container = os.environ.get("AZURE_CONTAINER", "mycontainer")

    hadoop_azure = "3.3.4"
    prefix = f"spark.hadoop.fs.azure.account"
    suffix = f"{account_name}.dfs.core.windows.net"

    spark = (SparkSession.builder
             .appName("azure-read-service-principal")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.jars.packages",
                     f"org.apache.hadoop:hadoop-azure:{hadoop_azure}")
             .config(f"{prefix}.auth.type.{suffix}", "OAuth")
             .config(f"{prefix}.oauth.provider.type.{suffix}",
                     "org.apache.hadoop.fs.azurebfs.oauth2"
                     ".ClientCredsTokenProvider")
             .config(f"{prefix}.oauth2.client.id.{suffix}", client_id)
             .config(f"{prefix}.oauth2.client.secret.{suffix}", client_secret)
             .config(f"{prefix}.oauth2.client.endpoint.{suffix}",
                     f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
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
