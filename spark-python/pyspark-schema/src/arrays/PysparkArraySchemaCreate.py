from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark_app = SparkSession.builder.appName('array-schema-fields').getOrCreate()