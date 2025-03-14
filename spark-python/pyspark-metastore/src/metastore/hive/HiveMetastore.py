from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark_session = (SparkSession.builder
                     .appName("hive-spark-schema")
                     .config('spark.hive.metastore.uris', 'thrift://localhost:9083')
                     .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                     .config("hive.metastore.uris", "thrift://localhost:9083")
                     .enableHiveSupport()
                     .getOrCreate())
