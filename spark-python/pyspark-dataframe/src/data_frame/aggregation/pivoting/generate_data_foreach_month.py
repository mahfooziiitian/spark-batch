"""
Distribute an annual policy premium evenly across each month it was active,
then pivot by policy number to produce one row per policy.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    schema = StructType(
        [
            StructField("Policy Number", StringType(), True),
            StructField("Start Date", StringType(), True),
            StructField("End Date", StringType(), True),
            StructField("Premium", IntegerType(), True),
        ]
    )

    data = [
        ("P1", "Jan, 2023", "Dec, 2023", 120),
        ("P2", "Jan, 2023", "Oct, 2023", 100),
        ("P3", "Jan, 2023", "May, 2023", 50),
    ]

    df = spark.createDataFrame(data, schema=schema)

    df = (
        df.withColumn("Start Date", F.to_date(F.col("Start Date"), "MMM, yyyy"))
        .withColumn("End Date", F.to_date(F.col("End Date"), "MMM, yyyy"))
        .withColumn("Premium", F.col("Premium").cast("int"))
    )

    df.show(truncate=False)

    df = (
        df.withColumn(
            "Months",
            F.expr("sequence(`Start Date`, `End Date`, interval 1 month)"),
        )
        .withColumn("End Date", F.explode(F.col("Months")))
        .withColumn("premium_per_month", F.col("Premium") / F.size(F.col("Months")))
        .select("Policy Number", "Start Date", "End Date", "premium_per_month")
    )

    df.show(truncate=False)

    df = df.groupBy("Policy Number").agg(
        F.sum("premium_per_month").alias("Premium"),
        F.lit(df.select("Start Date").first()[0]).alias("Start Date"),
        F.lit(
            df.select("End Date").orderBy("End Date", ascending=False).first()[0]
        ).alias("End Date"),
    )

    df.show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("generate-data-foreach-month")
    main(spark)
    spark.stop()
