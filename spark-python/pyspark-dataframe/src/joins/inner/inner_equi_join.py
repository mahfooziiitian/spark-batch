"""
INNER JOIN is commonly used when you want to retrieve data where a relationship exists between columns in different
tables.
It helps filter and combine rows based on specified conditions, ensuring that only matching rows are included in the
result set.
"""
import os
import sys

from pyspark.sql import SparkSession

from joins import join_data

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    spark = (SparkSession.builder.appName("Inner Equi-Join")
             .master("local[*]")
             .getOrCreate())

    employee_df = spark.createDataFrame(
        join_data.employee_data,
        join_data.employee_schema
    )
    # Create DataFrame
    department_df = spark.createDataFrame(join_data.department_data, schema=join_data.department_schema)

    employee_df.join(department_df, ["department_id"], "inner").show(truncate=False)
