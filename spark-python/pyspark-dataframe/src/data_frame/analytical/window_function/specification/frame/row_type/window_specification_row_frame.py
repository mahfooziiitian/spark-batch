"""
ROW frames use physical offsets from the current row.
CURRENT ROW, n PRECEDING, and n FOLLOWING specify exact row counts.
"""

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

    # Frame: current row and the next 2 rows (3-row look-ahead)
    window_spec = (
        Window.partitionBy("dept")
        .orderBy(F.col("salary"))
        .rowsBetween(Window.currentRow, 2)
    )

    salary_difference = F.max("salary").over(window_spec) - F.col("salary")

    (
        df.select(
            "name", "dept", "salary", salary_difference.alias("salary_difference")
        ).show(truncate=False)
    )


if __name__ == "__main__":
    spark = get_spark("window-row-frame")
    main(spark)
    spark.stop()
