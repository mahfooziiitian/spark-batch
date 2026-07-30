"""DataFrame joins on JSON data.

Demonstrates joining multiple JSON datasets using various join types.
A common pattern when JSON data is spread across multiple files or APIs.

Key concepts:
    - Inner, left, right, full outer, cross, semi, anti joins
    - Joining on single and multiple columns
    - Handling duplicate column names
    - Broadcast joins for small tables

Reference:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html
"""

from pyspark.sql import functions as F

from pys_json import (
    DATA_HOME,
    get_spark,
    print_dataframe,
    print_header,
    print_success,
    set_log_level,
    write_json_lines,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.joins")


if __name__ == "__main__":
    spark = get_spark("json-joins")

    # Create two JSON datasets
    emp_file = DATA_HOME + "/file_data/json/df_demo/join_employees.json"
    write_json_lines(
        emp_file,
        [
            '{"emp_id": 1, "name": "Alice", "dept_id": 10}',
            '{"emp_id": 2, "name": "Bob", "dept_id": 20}',
            '{"emp_id": 3, "name": "Charlie", "dept_id": 10}',
            '{"emp_id": 4, "name": "Diana", "dept_id": 30}',
            '{"emp_id": 5, "name": "Eve", "dept_id": 99}',
        ],
    )

    dept_file = DATA_HOME + "/file_data/json/df_demo/join_departments.json"
    write_json_lines(
        dept_file,
        [
            '{"dept_id": 10, "dept_name": "Engineering", "budget": 500000}',
            '{"dept_id": 20, "dept_name": "Marketing", "budget": 300000}',
            '{"dept_id": 30, "dept_name": "Sales", "budget": 250000}',
            '{"dept_id": 40, "dept_name": "HR", "budget": 200000}',
        ],
    )

    df_emp = spark.read.json(emp_file)
    df_dept = spark.read.json(dept_file)

    print_dataframe(df_emp, title="Employees")
    print_dataframe(df_dept, title="Departments")

    # =========================================================================
    # 1. Inner join
    # =========================================================================
    print_header("1. Inner Join")

    df_inner = df_emp.join(df_dept, "dept_id", "inner")
    print_dataframe(df_inner, title="Inner Join (matching rows only)")
    logger.info("Eve (dept_id=99) dropped — no matching department")
    logger.info("HR (dept_id=40) dropped — no matching employee")

    # =========================================================================
    # 2. Left outer join
    # =========================================================================
    print_header("2. Left Outer Join")

    df_left = df_emp.join(df_dept, "dept_id", "left")
    print_dataframe(df_left, title="Left Join (all employees)")
    print_success("Eve kept with null dept_name/budget")

    # =========================================================================
    # 3. Right outer join
    # =========================================================================
    print_header("3. Right Outer Join")

    df_right = df_emp.join(df_dept, "dept_id", "right")
    print_dataframe(df_right, title="Right Join (all departments)")
    print_success("HR dept kept with null employee data")

    # =========================================================================
    # 4. Full outer join
    # =========================================================================
    print_header("4. Full Outer Join")

    df_full = df_emp.join(df_dept, "dept_id", "full")
    print_dataframe(df_full, title="Full Outer Join (all rows)")

    # =========================================================================
    # 5. Left semi join (exists filter)
    # =========================================================================
    print_header("5. Left Semi Join (EXISTS)")

    df_semi = df_emp.join(df_dept, "dept_id", "left_semi")
    print_dataframe(df_semi, title="Semi Join (employees WITH departments)")
    print_success("Returns only employee columns — like WHERE EXISTS")

    # =========================================================================
    # 6. Left anti join (NOT EXISTS)
    # =========================================================================
    print_header("6. Left Anti Join (NOT EXISTS)")

    df_anti = df_emp.join(df_dept, "dept_id", "left_anti")
    print_dataframe(df_anti, title="Anti Join (employees WITHOUT departments)")
    print_success("Only Eve (dept_id=99) — no matching department")

    # =========================================================================
    # 7. Broadcast join (small table optimization)
    # =========================================================================
    print_header("7. Broadcast Join")

    df_broadcast = df_emp.join(F.broadcast(df_dept), "dept_id", "inner")
    print_dataframe(df_broadcast, title="Broadcast Join (dept table broadcasted)")
    print_success("broadcast() sends small table to all executors — avoids shuffle")

    spark.stop()
