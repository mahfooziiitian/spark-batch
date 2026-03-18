"""
Row creation — all patterns for constructing pyspark.sql.Row objects.

pyspark.sql.Row is both:
  • a namedtuple-like object used to represent a single DataFrame row
  • a factory for creating typed row subclasses via Row("field1", "field2")

Patterns covered:
  1. Named-keyword Row          — Row(id=1, name="Alice")
  2. Positional Row             — Row(1, "Alice") with implicit field names
  3. Named Row subclass         — Employee = Row("id", "name"); Employee(1, "Alice")
  4. Row from dict unpacking    — Row(**record)
  5. List of Rows → DataFrame   — spark.createDataFrame(rows)
  6. Row with None / NULL       — Row(id=1, dept=None)
  7. Row from RDD               — sparkContext.parallelize([Row(…)])
"""

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from data_frame.spark_utils import get_spark


def demo_named_kwargs(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 1. Named-keyword Row — field names are part of the Row object
    # ------------------------------------------------------------------
    row = Row(id=1, name="Alice", department="Engineering", salary=90000.0)

    print("=== Named-keyword Row ===")
    print(f"  row          : {row}")
    print(f"  row.id       : {row.id}")
    print(f"  row.name     : {row.name}")
    print(f"  row.__fields__: {row.__fields__}")  # ordered field names

    # Named-keyword Rows can be used directly to build a DataFrame
    data = [
        Row(id=1, name="Alice", salary=90000.0),
        Row(id=2, name="Bob", salary=75000.0),
        Row(id=3, name="Carol", salary=82000.0),
    ]
    df = spark.createDataFrame(data)  # schema inferred from Row fields
    df.printSchema()
    df.show(truncate=False)


def demo_positional(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 2. Positional Row — no field names embedded; schema supplied externally
    # ------------------------------------------------------------------
    row = Row(1, "Alice", 90000.0)

    print("=== Positional Row ===")
    print(f"  row    : {row}")
    print(f"  row[0] : {row[0]}")  # access only by index — no .name attribute
    print(f"  row[1] : {row[1]}")

    data = [Row(1, "Alice", 90000.0), Row(2, "Bob", 75000.0)]
    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("salary", DoubleType(), nullable=True),
        ]
    )
    df = spark.createDataFrame(data, schema)  # schema required for positional rows
    df.show(truncate=False)


def demo_namedtuple_factory(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 3. Named Row subclass — Row("f1", "f2") returns a namedtuple class.
    #    Instances get proper field names and attribute access.
    # ------------------------------------------------------------------
    Employee = Row("id", "name", "department")  # creates a class

    alice = Employee(1, "Alice", "Engineering")
    bob = Employee(2, "Bob", "Sales")

    print("=== Named Row subclass ===")
    print(f"  alice          : {alice}")
    print(f"  alice.name     : {alice.name}")
    print(f"  alice.__fields__: {alice.__fields__}")

    df = spark.createDataFrame([alice, bob])
    df.show(truncate=False)


def demo_from_dict(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 4. Row from dict unpacking — Row(**record)
    # ------------------------------------------------------------------
    records = [
        {"order_id": 101, "customer": "Alice", "amount": 299.99, "status": "active"},
        {"order_id": 102, "customer": "Bob", "amount": 149.50, "status": "active"},
        {"order_id": 103, "customer": "Carol", "amount": 49.00, "status": "pending"},
    ]

    rows = [Row(**r) for r in records]
    print("=== Row from dict unpacking ===")
    print(f"  rows[0] : {rows[0]}")

    df = spark.createDataFrame(rows)
    df.show(truncate=False)


def demo_null_fields(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 5. Row with None — becomes NULL in the DataFrame
    # ------------------------------------------------------------------
    data = [
        Row(id=1, name="Alice", manager_id=None),  # NULL manager
        Row(id=2, name="Bob", manager_id=1),
        Row(id=3, name="Carol", manager_id=1),
    ]
    print("=== Row with None (NULL) ===")
    df = spark.createDataFrame(data)
    df.show(truncate=False)
    print(f"  NULL manager_id count: {df.filter(df.manager_id.isNull()).count()}")


def demo_from_rdd(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 6. Row from RDD — parallelize a list of Row objects
    # ------------------------------------------------------------------
    Employee = Row("id", "name", "score")
    rdd = spark.sparkContext.parallelize(
        [
            Employee(1, "Alice", 95.5),
            Employee(2, "Bob", 87.0),
            Employee(3, "Carol", 91.3),
        ]
    )
    df = spark.createDataFrame(rdd)
    print("=== Row from RDD ===")
    df.show(truncate=False)


def main(spark: SparkSession) -> None:
    demo_named_kwargs(spark)
    demo_positional(spark)
    demo_namedtuple_factory(spark)
    demo_from_dict(spark)
    demo_null_fields(spark)
    demo_from_rdd(spark)


if __name__ == "__main__":
    spark = get_spark("row-creation")
    main(spark)
    spark.stop()
