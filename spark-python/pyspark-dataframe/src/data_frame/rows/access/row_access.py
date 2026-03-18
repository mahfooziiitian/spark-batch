"""
Row field access — every way to read values from a collected Row.

Patterns covered:
  1. Attribute access         — row.name
  2. Dict-style access        — row["name"]
  3. Index access             — row[0]
  4. asDict()                 — whole row as a plain dict
  5. Safe attribute check     — hasattr(row, "field")
  6. Safe access with default — getattr(row, "field", default)
  7. __fields__               — ordered list of field names
  8. Iteration                — for value in row
  9. len(row)                 — number of fields
 10. Unpacking                — a, b, c = row
"""

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from data_frame.sample_data import employees
from data_frame.spark_utils import get_spark


def demo_attribute_and_dict(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees())
    row = df.orderBy("id").first()

    # ------------------------------------------------------------------
    # 1. Attribute access — row.field_name
    # ------------------------------------------------------------------
    print("=== Attribute access ===")
    print(f"  row.id            : {row.id}")
    print(f"  row.employee_name : {row.employee_name}")
    print(f"  row.department_id : {row.department_id}")

    # ------------------------------------------------------------------
    # 2. Dict-style access — row["field_name"]  (same result, different syntax)
    # ------------------------------------------------------------------
    print("\n=== Dict-style access ===")
    print(f"  row['id']           : {row['id']}")
    print(f"  row['employee_name']: {row['employee_name']}")


def demo_index_access(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees())
    row = df.orderBy("id").first()

    # ------------------------------------------------------------------
    # 3. Index access — row[n]  (0-based, column order matches schema)
    # ------------------------------------------------------------------
    print("\n=== Index access ===")
    for i in range(len(row)):
        print(f"  row[{i}] = {row[i]!r}")


def demo_as_dict(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees())
    row = df.orderBy("id").first()

    # ------------------------------------------------------------------
    # 4. asDict() — convert whole Row to an OrderedDict
    # ------------------------------------------------------------------
    d = row.asDict()
    print("\n=== asDict() ===")
    print(f"  type   : {type(d)}")
    print(f"  dict   : {d}")
    print(f"  keys   : {list(d.keys())}")

    # Useful for safe field existence check
    print(f"  'id' in dict        : {'id' in d}")
    print(f"  'missing' in dict   : {'missing' in d}")

    # Convert all collected rows to dicts
    all_dicts = [r.asDict() for r in df.collect()]
    print(f"\n  all rows as dicts ({len(all_dicts)} rows):")
    for item in all_dicts:
        print(f"    {item}")


def demo_safe_access(spark: SparkSession) -> None:
    row = Row(id=1, name="Alice", department_id=None)

    # ------------------------------------------------------------------
    # 5. hasattr — check whether a field exists on the Row
    # ------------------------------------------------------------------
    print("\n=== hasattr ===")
    print(f"  hasattr(row, 'id')       : {hasattr(row, 'id')}")
    print(
        f"  hasattr(row, 'salary')   : {hasattr(row, 'salary')}"
    )  # False — not present

    # ------------------------------------------------------------------
    # 6. getattr with default — safe access when field may be missing
    # ------------------------------------------------------------------
    print("\n=== getattr with default ===")
    name = getattr(row, "name", "UNKNOWN")
    salary = getattr(row, "salary", 0.0)  # field doesn't exist → default
    dept_id = getattr(row, "department_id", -1)  # field exists but is None

    print(f"  name     : {name}")
    print(f"  salary   : {salary}")
    print(f"  dept_id  : {dept_id}")  # None — getattr returns the None, not -1


def demo_fields_and_iteration(spark: SparkSession) -> None:
    row = Row(id=1, name="Alice", salary=90000.0, active=True)

    # ------------------------------------------------------------------
    # 7. __fields__ — ordered tuple of field names (named Rows only)
    # ------------------------------------------------------------------
    print("\n=== __fields__ ===")
    print(f"  row.__fields__ : {row.__fields__}")

    # ------------------------------------------------------------------
    # 8. Iteration — yields field values in schema order
    # ------------------------------------------------------------------
    print("\n=== Iteration ===")
    for field, value in zip(row.__fields__, row):
        print(f"  {field:10s} = {value!r}")

    # ------------------------------------------------------------------
    # 9. len() — number of fields
    # ------------------------------------------------------------------
    print("\n=== len(row) ===")
    print(f"  len(row) : {len(row)}")

    # ------------------------------------------------------------------
    # 10. Tuple unpacking
    # ------------------------------------------------------------------
    print("\n=== Tuple unpacking ===")
    id_, name_, salary_, active_ = row
    print(f"  id={id_}, name={name_}, salary={salary_}, active={active_}")


def demo_row_from_collect(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees()).select("id", "employee_name").orderBy("id")

    print("\n=== Rows from collect() ===")
    for row in df.collect():
        # Mix attribute and dict access freely
        print(f"  id={row.id:>2}  name={row['employee_name']}")


def main(spark: SparkSession) -> None:
    demo_attribute_and_dict(spark)
    demo_index_access(spark)
    demo_as_dict(spark)
    demo_safe_access(spark)
    demo_fields_and_iteration(spark)
    demo_row_from_collect(spark)


if __name__ == "__main__":
    spark = get_spark("row-access")
    main(spark)
    spark.stop()
