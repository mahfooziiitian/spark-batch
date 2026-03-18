"""
INNER non-equi JOIN — joins on a compound condition that includes a range predicate.
Useful when the join condition is not a simple equality check.
"""

from pyspark.sql import functions as F

from data_frame.sample_data import employees, salaries
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    employee_df = spark.createDataFrame(*employees())
    salary_df = spark.createDataFrame(*salaries())

    join_condition = (salary_df["employee_id"] == employee_df["id"]) & salary_df[
        "current_salary"
    ].between(50000, 75000)

    result = employee_df.join(salary_df, join_condition, "inner").select(
        employee_df["*"], salary_df["current_salary"]
    )
    result.show(truncate=False)
    result.explain()


if __name__ == "__main__":
    spark = get_spark("inner-non-equi-join")
    main(spark)
    spark.stop()
