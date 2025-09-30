from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("HadoopCatalog")
    .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
    .config(
        "spark.sql.catalog.hadoop_catalog.warehouse", "hdfs://namenode:8020/warehouse"
    )
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.shuffle.partitions", "8")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("SparkSession started with HadoopCatalog and Iceberg extensions.")
