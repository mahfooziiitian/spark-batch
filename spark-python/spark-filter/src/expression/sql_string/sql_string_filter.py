from pyspark.sql import SparkSession


def main():

    spark = SparkSession.builder.appName("").getOrCreate()

    data = [
        (1, {"name": "Alice", "age": 25}),
        (2, {"name": "Bob", "age": 30}),
        (3, {"name": "Charlie", "age": 35}),
    ]

    df = spark.createDataFrame(data, ["id", "people"])

    df.filter("age > 25 AND department = 'Sales'")


if __name__ == "__main__":
    main()
