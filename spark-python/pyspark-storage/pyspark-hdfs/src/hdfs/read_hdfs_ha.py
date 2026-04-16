import os

from pyspark.sql import SparkSession

if __name__ == "__main__":
    os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME_11", "/usr/lib/jvm/java-8-openjdk-amd64")
    nn1 = os.environ.get("HDFS_NN1", "localhost:8020")
    nn2 = os.environ.get("HDFS_NN2", "localhost:8021")

    spark = (
        SparkSession.builder.appName("hdfs-read-ha")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.hadoop.fs.defaultFS", "hdfs://mycluster")
        .config("spark.hadoop.dfs.nameservices", "mycluster")
        .config("spark.hadoop.dfs.ha.namenodes.mycluster", "nn1,nn2")
        .config("spark.hadoop.dfs.namenode.rpc-address.mycluster.nn1", nn1)
        .config("spark.hadoop.dfs.namenode.rpc-address.mycluster.nn2", nn2)
        .config(
            "spark.hadoop.dfs.client.failover.proxy.provider.mycluster",
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    input_path = os.environ.get(
        "INPUT_PATH", "hdfs://mycluster/user/data/input/sample.csv")
    output_path = os.environ.get(
        "OUTPUT_PATH", "hdfs://mycluster/user/data/output")

    df = spark.read.option("inferSchema", True).option("header", True).csv(input_path)
    df.show(truncate=False)

    df.write.mode("overwrite").parquet(output_path)

    spark.stop()
