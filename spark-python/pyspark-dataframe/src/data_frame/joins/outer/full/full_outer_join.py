"""
FULL OUTER JOIN — returns all rows from both DataFrames.
Non-matching columns from either side are filled with null.
Use when you need the complete dataset from both tables regardless of whether a match exists.
"""

from data_frame.sample_data import departments, employees
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    employee_df = spark.createDataFrame(*employees())
    department_df = spark.createDataFrame(*departments())

    result = employee_df.join(department_df, on=["department_id"], how="full")
    result.show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("full-outer-join")
    main(spark)
    spark.stop()
