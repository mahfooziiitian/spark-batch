from pyspark.sql import SparkSession


# Enhanced SparkSession builder with additional configurations and error handling
def create_spark_session():
    try:
        spark = (
            SparkSession.builder.appName("RESTCatalog")
            .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.rest.type", "rest")
            .config("spark.sql.catalog.rest.uri", "https://metastore-api.example.com")
            .config("spark.sql.catalog.rest.warehouse", "s3://my-bucket/warehouse")
            .config(
                "spark.sql.catalog.rest.credential", "bearer-token"
            )  # Fixed typo in config key
            .config(
                "spark.sql.shuffle.partitions", "200"
            )  # Example: optimize shuffle partitions
            .config("spark.executor.memory", "4g")  # Example: set executor memory
            .config("spark.driver.memory", "2g")  # Example: set driver memory
            .enableHiveSupport()  # Enable Hive support if needed
            .getOrCreate()
        )
        print("SparkSession created successfully.")
        return spark
    except Exception as e:
        print(f"Error creating SparkSession: {e}")
        raise


# Usage
spark = create_spark_session()
