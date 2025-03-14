import os
import sys

from pyspark.sql import SparkSession

from joins import join_data

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    spark = (SparkSession.builder.appName("Left Outer Join")
             .master("local[*]")
             .getOrCreate())

    employee_df = spark.createDataFrame(
        join_data.employee_data,
        join_data.employee_schema
    )
    # Create DataFrame
    department_df = spark.createDataFrame(join_data.department_data, schema=join_data.department_schema)

    employee_df.join(department_df, ["department_id"], "left").show(truncate=False)
