import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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
    salary_df = spark.createDataFrame(join_data.salary_data, schema=join_data.salary_schema)

    join_condition = (salary_df["employee_id"] == employee_df["id"]) & (salary_df["current_salary"].between(4000, 5000))

    employee_salary_df = employee_df.join(salary_df, join_condition, "inner").select(employee_df["*"],
                                                                                     salary_df["current_salary"])

    employee_salary_df.explain(extended=True)

