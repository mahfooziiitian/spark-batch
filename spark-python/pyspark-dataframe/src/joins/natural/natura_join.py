"""
A NATURAL JOIN is a type of JOIN that combines tables based on columns with the same name and data type.
When you use the NATURAL JOIN clause, it creates an implicit JOIN clause for you based on the common columns in the two
tables being joined.

Natural joins can be less explicit than specifying the join conditions manually, and they rely on columns having the
same name and compatible data types.
If you have multiple columns with the same name in both DataFrames, they will all be used as join conditions.
Ensure your data and use case are suitable for a natural join.

"""
import os
import sys

from pyspark.sql import SparkSession

from joins import join_data

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    spark = (SparkSession.builder.appName("Natural Join")
             .master("local[*]")
             .getOrCreate())

    employee_df = spark.createDataFrame(
        join_data.employee_data,
        join_data.employee_schema
    )

    employee_df.createOrReplaceTempView("employees")

    # Create DataFrame
    department_df = spark.createDataFrame(join_data.department_data, schema=join_data.department_schema)

    department_df.createOrReplaceTempView("departments")

    employee_df.join(department_df, ["department_id"], "inner").show(truncate=False)

    spark.sql("SELECT * FROM employees NATURAL JOIN departments").show(truncate=False)
