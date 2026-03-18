from pyspark.sql import functions as F

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    data = [
        ("Alice", "Category1", 10),
        ("Bob", "Category2", 20),
        ("Alice", "Category3", 30),
        ("Bob", "Category1", 40),
        ("Alice", "Category2", 50),
        ("Bob", "Category3", 60),
    ]
    df = spark.createDataFrame(data, ["Name", "Category", "Value"])
    df.show()

    result_df = df.groupBy("Name").pivot("Category").agg({"Value": "sum"}).fillna(0)
    result_df.show()

    # Dynamic pivot — values discovered at runtime
    categories = df.select("Category").distinct().rdd.flatMap(lambda x: x).collect()
    pivot_expr = [F.col(cat) for cat in categories]
    dynamic_df = df.groupBy("Name").pivot("Category", categories).sum("Value").fillna(0)
    dynamic_df.select("Name", *pivot_expr).show()


if __name__ == "__main__":
    spark = get_spark("dynamic-pivot")
    main(spark)
    spark.stop()
