"""
Now, if you want to retrieve a list of all employees and their respective departments, you can use a FULL OUTER JOIN.
including those employees who are not assigned to any department and departments with no associated employees.

The result of this query would include all records from both tables, combining them based on the department_id.
If there is no match for a particular employee or department, NULL values will be present for the columns from the
non-matching table.

A FULL OUTER JOIN is used when you want to retrieve all records from both tables, regardless of whether there is a match
in the join condition.
This type of join is useful in scenarios where you want to see the complete set of data from two tables,
including the unmatched rows from either side.

"""
import os
import sys

from pyspark.sql import SparkSession

from joins import join_data

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    spark = (SparkSession.builder.appName("Full Outer Join")
             .master("local[*]")
             .getOrCreate())

    employee_df = spark.createDataFrame(
        join_data.employee_data,
        join_data.employee_schema
    )
    # Create DataFrame
    department_df = spark.createDataFrame(join_data.department_data, schema=join_data.department_schema)

    employee_df.join(department_df, ["department_id"], "full_outer").show(truncate=False)

