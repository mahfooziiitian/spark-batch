"""
SELF JOIN — joins a DataFrame to itself to resolve hierarchical relationships.
Always alias both sides to avoid ambiguous column references.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    schema = StructType(
        [
            StructField("employee_id", IntegerType(), nullable=False),
            StructField("employee_name", StringType(), nullable=True),
            StructField("manager_id", IntegerType(), nullable=True),
        ]
    )
    data = [
        (1, "Homer Simpson", None),
        (2, "Ned Flanders", 1),
        (3, "Barney Gumble", 2),
        (4, "Alice Brown", 1),
    ]
    df = spark.createDataFrame(data, schema)
    df.show(truncate=False)

    emp = df.alias("emp")
    mgr = df.alias("mgr")

    result = (
        emp.join(mgr, F.col("emp.manager_id") == F.col("mgr.employee_id"), how="left")
        .select(
            F.col("emp.employee_id"),
            F.col("emp.employee_name"),
            F.col("mgr.employee_name").alias("manager_name"),
        )
        .orderBy("emp.employee_id")
    )
    result.show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("self-join")
    main(spark)
    spark.stop()
