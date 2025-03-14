"""
Table 1

Policy Number	Start Date	End Date	Premium
P1	Jan, 2023	Dec,2023	120
P2	Jan, 2023	Oct,2023	100
P3	Jan, 2023	May,2023	50

Table 2

Policy Number	Start Date	End Date	Premium
P1	Jan, 2023	Feb,2023	10
P1	Feb,2023	Mar,2023	10
P1	Mar,2023	Apr,2023	10
P1	Apr,2023	May,2023	10
P1	May,2023	June,2023	10
P1	June,2023	July,2023	10
P1	July,2023	Aug,2023	10
P1	Aug,2023	Sep,2023	10
P1	Sep,2023	Oct,2023	10
P1	Oct,2023	Nov,2023	10
P1	Nov,2023	Dec.2023	10
"""
import os
import sys

from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import explode, expr, to_date, col, size, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    spark = SparkSession.builder.appName("DynamicPivot").getOrCreate()

    # Define the schema for the DataFrame
    schema = StructType([
        StructField("Policy Number", StringType(), True),
        StructField("Start Date", StringType(), True),
        StructField("End Date", StringType(), True),
        StructField("Premium", IntegerType(), True)
    ])

    # Data from Table 1
    data = [
        ("P1", "Jan, 2023", "Dec, 2023", 120),
        ("P2", "Jan, 2023", "Oct, 2023", 100),
        ("P3", "Jan, 2023", "May, 2023", 50)
    ]

    # Create the DataFrame
    df = spark.createDataFrame(data, schema=schema)

    # Convert "Start Date" and "End Date" columns to date type
    df = df.withColumn("Start Date", to_date(df["Start Date"], 'MMM, yyyy'))
    df = (df.withColumn("End Date", to_date(df["End Date"], 'MMM, yyyy'))
          .withColumn("Premium", df["Premium"].cast("int")))

    # Show the DataFrame
    df.show(truncate=False)

    # Generate a sequence of months between "Start Date" and "End Date" for each policy
    df = (df
          .withColumn("Months", expr("sequence(to_date(`Start Date`, 'MMM, yyyy'), to_date(`End Date`, 'MMM, "
                                     "yyyy'), interval 1 month)"))
          .withColumn("End Date", explode(col("Months")))
          .withColumn("premium_per_month", col("Premium") / size(col("Months")))
          .select("Policy Number", "Start Date", "End Date", "premium_per_month")
          # .filter(col("Start Date") != col("End Date"))
          )

    df.show(truncate=False)

    # Pivot the DataFrame
    # Aggregate premiums for each policy number
    df = df.groupBy("Policy Number").agg(
        functions.sum("premium_per_month").alias("Premium"),
        lit(df.select("Start Date").first()[0]).alias("Start Date"),
        lit(df.select("End Date").orderBy("End Date", ascending=False).first()[0]).alias("End Date")
    )

    # Show the aggregated DataFrame
    df.show(truncate=False)
