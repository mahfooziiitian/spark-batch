"""
INNER JOIN — returns only rows that have a matching key in both DataFrames.
Use it when you want to discard non-matching rows from both sides.
"""

from data_frame.sample_data import departments, employees
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    employee_df = spark.createDataFrame(*employees())
    department_df = spark.createDataFrame(*departments())

    result = employee_df.join(department_df, on=["department_id"], how="inner")
    result.show(truncate=False)
    result.explain()


if __name__ == "__main__":
    spark = get_spark("inner-equi-join")
    main(spark)
    spark.stop()
