"""Query text data using Spark SQL by creating temporary views."""
import os
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("text_sql_view")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    tmp = os.path.join(tempfile.mkdtemp(), "employees.txt")
    with open(tmp, "w") as f:
        f.write("E001|Alice|Engineering|95000\n")
        f.write("E002|Bob|Marketing|72000\n")
        f.write("E003|Charlie|Engineering|105000\n")
        f.write("E004|Diana|Sales|68000\n")
        f.write("E005|Eve|Engineering|88000\n")
        f.write("E006|Frank|Marketing|75000\n")

    # read and parse
    df = spark.read.text(tmp)
    df_parsed = df.select(
        F.split("value", "\\|")[0].alias("emp_id"),
        F.split("value", "\\|")[1].alias("name"),
        F.split("value", "\\|")[2].alias("department"),
        F.split("value", "\\|")[3].cast("int").alias("salary"),
    )

    # register as temp view
    df_parsed.createOrReplaceTempView("employees")

    print("=== All employees ===")
    spark.sql("SELECT * FROM employees").show()

    print("=== Average salary by department ===")
    spark.sql("""
        SELECT department,
               COUNT(*) AS headcount,
               ROUND(AVG(salary), 2) AS avg_salary,
               MAX(salary) AS max_salary
        FROM employees
        GROUP BY department
        ORDER BY avg_salary DESC
    """).show()

    print("=== Employees earning above average ===")
    spark.sql("""
        SELECT name, department, salary
        FROM employees
        WHERE salary > (SELECT AVG(salary) FROM employees)
        ORDER BY salary DESC
    """).show()

    # --- direct SQL text read (Spark 3.x) ---
    print("=== Direct SQL read from path ===")
    spark.sql(f"""
        SELECT *
        FROM text.`{tmp}`
    """).show(truncate=False)

    spark.stop()
