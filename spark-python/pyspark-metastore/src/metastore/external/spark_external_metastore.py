from pyspark.sql import SparkSession


def create_spark_session(app_name: str, metastore_url: str):
    return (
        SparkSession.builder.appName(app_name)
        .config("javax.jdo.option.ConnectionURL", metastore_url)
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )


if __name__ == "__main__":
    spark = create_spark_session(
        app_name="SparkSQLExample",
        metastore_url="jdbc:mysql://localhost:3306/my_metastore_db",
    )
    print("Spark session started with Hive metastore support.")
    print("Spark version:", spark.version)
    # Example: List databases
    print("Databases:", spark.sql("SHOW DATABASES").show())
