"""
An SQL CROSS JOIN is used when you need to find out all the possibilities of combining two tables, where the result set
includes every row from each contributing table.
The CROSS JOIN clause returns the Cartesian product of rows from the joined tables.
"""
import os
import sys

from pyspark.sql import SparkSession

from joins import join_data

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    spark = (SparkSession.builder
             .appName("cross_join")
             .master("local[*]")
             .getOrCreate())

    employee_df = spark.createDataFrame(
        join_data.employee_data,
        join_data.employee_schema
    )
    # Create DataFrame
    department_df = spark.createDataFrame(join_data.department_data, schema=join_data.department_schema)

    employee_df.crossJoin(department_df).show(truncate=False)
