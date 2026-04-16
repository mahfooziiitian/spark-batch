import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == '__main__':
    account_name = os.environ.get("AZURE_STORAGE_ACCOUNT", "mystorageaccount")
    account_key = os.environ.get("AZURE_STORAGE_KEY", "")
    container = os.environ.get("AZURE_CONTAINER", "mycontainer")

    base_url = f"abfss://{container}@{account_name}.dfs.core.windows.net"

    hadoop_azure = "3.3.4"
    spark = (SparkSession.builder
             .appName("azure-write-parquet")
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
        f"{base_url}/input/sample.csv")
    output_path = os.environ.get(
        "OUTPUT_PATH",
        f"{base_url}/output/dept_avg_salary")

    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
    df.show(truncate=False)

    dept_avg = (df
                .groupBy("department")
                .agg(F.avg("salary").alias("avg_salary")))
    dept_avg.show(truncate=False)

    dept_avg.write.mode("overwrite").parquet(output_path)
    print(f"Parquet written to {output_path}")

    spark.stop()
