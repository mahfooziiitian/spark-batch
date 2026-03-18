"""
BROADCAST JOIN — forces the smaller DataFrame to be broadcast to every executor,
eliminating the shuffle that a standard join would require.
Use F.broadcast() when one side is small enough to fit in executor memory.
"""

from pyspark.sql import functions as F

from data_frame.sample_data import departments, employees
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    employee_df = spark.createDataFrame(*employees())
    department_df = spark.createDataFrame(*departments())

    # F.broadcast() tells the optimizer to send department_df to every executor
    result = employee_df.join(
        F.broadcast(department_df), on=["department_id"], how="inner"
    )
    result.show(truncate=False)

    # BroadcastHashJoin should appear in the physical plan
    result.explain()


if __name__ == "__main__":
    spark = get_spark("broadcast-hash-join")
    main(spark)
    spark.stop()
