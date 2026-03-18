"""
NATURAL JOIN — implicitly joins on all columns that share the same name and type in both DataFrames.
The shared columns appear only once in the result.
Prefer explicit join keys in production; use natural joins in SQL contexts for brevity.
"""

from data_frame.sample_data import departments, employees
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    employee_df = spark.createDataFrame(*employees())
    department_df = spark.createDataFrame(*departments())

    # DataFrame API natural join
    result = employee_df.join(department_df, how="natural")
    result.show(truncate=False)

    # Equivalent Spark SQL natural join
    employee_df.createOrReplaceTempView("employees")
    department_df.createOrReplaceTempView("departments")
    spark.sql("SELECT * FROM employees NATURAL JOIN departments").show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("natural-join")
    main(spark)
    spark.stop()
