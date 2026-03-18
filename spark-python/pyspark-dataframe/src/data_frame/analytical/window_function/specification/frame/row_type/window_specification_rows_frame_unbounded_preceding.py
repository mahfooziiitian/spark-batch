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

    # ROW frame from the start of the partition to the current row (descending salary)
    window_spec = (
        Window.partitionBy("dept")
        .orderBy(F.col("salary").desc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    salary_difference = F.max("salary").over(window_spec) - F.col("salary")

    (
        df.select(
            "name", "dept", "salary", salary_difference.alias("salary_difference")
        ).show(truncate=False)
    )


if __name__ == "__main__":
    spark = get_spark("window-rows-unbounded-preceding")
    main(spark)
    spark.stop()
