from pyspark.sql import Row
from pyspark.sql.functions import expr

if __name__ == '__main__':
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

    # Create the employees DataFrame
    employees_data = [
        Row(employee_id=1, employee_name="John Doe", manager_id=None),
        Row(employee_id=2, employee_name="Jane Smith", manager_id=1),
        Row(employee_id=3, employee_name="Bob Johnson", manager_id=2),
        Row(employee_id=4, employee_name="Alice Brown", manager_id=1),
    ]

    employees_schema = ["employee_id", "employee_name", "manager_id"]
    employees_df = spark.createDataFrame(employees_data, schema=employees_schema)

    # Self-join to get employees and their managers
    self_joined_df = employees_df.alias("emp1").join(
        employees_df.alias("emp2"),
        employees_df["employee_id"] == employees_df["manager_id"],
        how="inner"
    )

    # Select relevant columns for display
    result_df = self_joined_df.select(
        "emp1.employee_id",
        "emp1.employee_name",
        "emp1.manager_id",
        expr("emp2.employee_name as manager_name")
    )

    # Show the resulting DataFrame
    result_df.show()
