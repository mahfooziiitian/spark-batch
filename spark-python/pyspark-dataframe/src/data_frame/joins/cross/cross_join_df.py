"""
CROSS JOIN variant using DataFrame API join method with employee/department data.
Every employee row is combined with every department row.
"""

from data_frame.sample_data import departments, employees
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    employee_df = spark.createDataFrame(*employees())
    department_df = spark.createDataFrame(*departments())

    result = employee_df.crossJoin(department_df)
    print(f"Cross-join row count: {result.count()}")
    result.show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("cross-join-df")
    main(spark)
    spark.stop()
