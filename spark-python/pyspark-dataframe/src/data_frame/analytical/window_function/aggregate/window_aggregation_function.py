from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    data = [
        ("Alice", "Engineering", 90000.0),
        ("Bob", "Engineering", 75000.0),
        ("Charlie", "Engineering", 82000.0),
        ("David", "Sales", 60000.0),
        ("Eve", "Sales", 55000.0),
        ("Frank", "HR", 70000.0),
    ]
    df = spark.createDataFrame(data, ["name", "dept", "salary"])

    window_spec = Window.partitionBy("dept")

    (
        df.select(
            F.col("name"),
            F.col("dept"),
            F.col("salary"),
            F.min("salary").over(window_spec).alias("min_salary"),
            F.avg("salary").over(window_spec).alias("avg_salary"),
            F.max("salary").over(window_spec).alias("max_salary"),
        ).show(truncate=False)
    )


if __name__ == "__main__":
    spark = get_spark("window-aggregation")
    main(spark)
    spark.stop()
