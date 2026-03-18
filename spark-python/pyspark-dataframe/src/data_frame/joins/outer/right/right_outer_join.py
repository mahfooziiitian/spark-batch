"""
RIGHT OUTER JOIN — returns all rows from the right DataFrame plus matching rows from the left.
Non-matching left-side columns are filled with null.
"""

from data_frame.sample_data import departments, employees
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    employee_df = spark.createDataFrame(*employees())
    department_df = spark.createDataFrame(*departments())

    result = employee_df.join(department_df, on=["department_id"], how="right")
    result.show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("right-outer-join")
    main(spark)
    spark.stop()
