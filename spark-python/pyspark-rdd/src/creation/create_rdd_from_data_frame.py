from pyspark import SparkContext
from pyspark.sql import SparkSession


def get_spark_context(app_name="RDD Example"):
    sc = SparkContext("local", app_name)
    spark = SparkSession(sc)
    return sc, spark


def create_rdd_from_list(sc, data):
    return sc.parallelize(data)


def create_rdd_from_file(sc, file_path, num_lines=5):
    rdd = sc.textFile(file_path)
    return rdd.take(num_lines)


def create_rdd_from_dataframe(spark, data, columns):
    df = spark.createDataFrame(data, columns)
    return df.rdd


def main():
    sc, spark = get_spark_context()
    # RDD from DataFrame
    df_data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    columns = ["id", "name"]
    rdd_from_df = create_rdd_from_dataframe(spark, df_data, columns)
    print("RDD from DataFrame:", rdd_from_df.collect())

    sc.stop()


if __name__ == "__main__":
    main()
