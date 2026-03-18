"""
Row conversion — transforming Row objects to and from other Python types.

Patterns covered:
  1. Row → dict              — asDict(), asDict(recursive=True)
  2. dict → Row              — Row(**d)
  3. Row → tuple             — tuple(row)
  4. Row → list              — list(row)
  5. Row → named fields zip  — dict(zip(row.__fields__, row))
  6. Rows → list[dict]       — [r.asDict() for r in df.collect()]
  7. list[dict] → DataFrame  — spark.createDataFrame(list_of_dicts)
  8. Row → JSON string       — json.dumps(row.asDict())
  9. Normalise nulls         — replace None values in a Row dict
 10. Flatten nested Row      — recursive dict flattening
"""

import json

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from data_frame.sample_data import customer_orders, employees
from data_frame.spark_utils import get_spark


def demo_row_to_dict(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees())
    row = df.orderBy("id").first()

    # ------------------------------------------------------------------
    # 1. asDict() — shallow conversion
    # ------------------------------------------------------------------
    d = row.asDict()
    print("=== Row → dict ===")
    print(f"  {d}")
    print(f"  type: {type(d).__name__}")

    # Modify the copy — does not affect the original Row (Row is immutable)
    d["employee_name"] = d["employee_name"].upper()
    print(f"  modified copy: {d}")
    print(f"  original row : {row.employee_name}")


def demo_dict_to_row(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 2. dict → Row — Row(**d) unpacking
    # ------------------------------------------------------------------
    records = [
        {"id": 10, "product": "Widget", "price": 9.99, "in_stock": True},
        {"id": 11, "product": "Gadget", "price": 49.99, "in_stock": False},
        {"id": 12, "product": "Dongle", "price": 4.99, "in_stock": True},
    ]
    rows = [Row(**r) for r in records]
    print("\n=== dict → Row ===")
    for r in rows:
        print(f"  {r}")

    df = spark.createDataFrame(rows)
    df.show(truncate=False)


def demo_row_to_tuple_and_list(spark: SparkSession) -> None:
    row = Row(id=1, name="Alice", salary=90000.0)

    # ------------------------------------------------------------------
    # 3. Row → tuple
    # ------------------------------------------------------------------
    t = tuple(row)
    print("\n=== Row → tuple ===")
    print(f"  {t}  type={type(t).__name__}")

    # ------------------------------------------------------------------
    # 4. Row → list
    # ------------------------------------------------------------------
    lst = list(row)
    print(f"\n=== Row → list ===")
    print(f"  {lst}  type={type(lst).__name__}")


def demo_row_to_named_dict(spark: SparkSession) -> None:
    row = Row(id=1, name="Alice", salary=90000.0, dept="Engineering")

    # ------------------------------------------------------------------
    # 5. Manual dict reconstruction — preserves field order
    # ------------------------------------------------------------------
    d = dict(zip(row.__fields__, row))
    print("\n=== dict(zip(__fields__, row)) ===")
    print(f"  {d}")

    # Useful when you want to select a subset of fields
    subset = {k: row[k] for k in ("id", "name")}
    print(f"  subset: {subset}")


def demo_collect_to_dicts(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    # ------------------------------------------------------------------
    # 6. List of collected rows → list of dicts
    # ------------------------------------------------------------------
    records = [r.asDict() for r in df.collect()]
    print(f"\n=== collect → list[dict] ({len(records)} records) ===")
    for rec in records[:3]:
        print(f"  {rec}")


def demo_dicts_to_dataframe(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 7. list[dict] → DataFrame  (schema inferred from first dict)
    # ------------------------------------------------------------------
    data = [
        {"region": "North", "quarter": "Q1", "revenue": 15000.0},
        {"region": "North", "quarter": "Q2", "revenue": 18000.0},
        {"region": "South", "quarter": "Q1", "revenue": 12000.0},
    ]
    df = spark.createDataFrame(data)
    print("\n=== list[dict] → DataFrame ===")
    df.printSchema()
    df.show(truncate=False)


def demo_row_to_json(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees())

    # ------------------------------------------------------------------
    # 8. Row → JSON string via asDict() + json.dumps
    # ------------------------------------------------------------------
    print("\n=== Row → JSON ===")
    for row in df.orderBy("id").collect():
        print(f"  {json.dumps(row.asDict())}")

    # Round-trip: JSON string → dict → Row
    json_str = json.dumps({"id": 99, "employee_name": "Test", "department_id": 1})
    restored_row = Row(**json.loads(json_str))
    print(f"\n  JSON → Row: {restored_row}")


def demo_normalise_nulls(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    # ------------------------------------------------------------------
    # 9. Replace None values in collected row dicts with typed defaults
    # ------------------------------------------------------------------
    defaults = {
        "customer_id": 0,
        "product": "UNKNOWN",
        "quantity": 0,
        "unit_price": 0.0,
        "status": "unknown",
    }

    def normalise(row: Row) -> dict:
        d = row.asDict()
        return {k: (d[k] if d[k] is not None else defaults.get(k)) for k in d}

    print("\n=== Normalise NULLs in collected rows ===")
    for norm in [normalise(r) for r in df.collect()]:
        print(f"  {norm}")


def demo_flatten_nested(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 10. Flatten a nested Row dict (struct columns) into a flat dict
    # ------------------------------------------------------------------

    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField(
                "address",
                StructType(
                    [
                        StructField("city", StringType(), nullable=True),
                        StructField("country", StringType(), nullable=True),
                    ]
                ),
                nullable=True,
            ),
        ]
    )
    data = [
        (1, "Alice", ("London", "UK")),
        (2, "Bob", ("Berlin", "DE")),
        (3, "Carol", ("Toronto", "CA")),
    ]
    df = spark.createDataFrame(data, schema)

    def flatten_dict(d: dict, prefix: str = "") -> dict:
        result = {}
        for k, v in d.items():
            full_key = f"{prefix}{k}"
            if isinstance(v, dict):
                result.update(flatten_dict(v, prefix=f"{full_key}_"))
            else:
                result[full_key] = v
        return result

    print("\n=== Flatten nested Row ===")
    for row in df.collect():
        nested = row.asDict(recursive=True)
        flat = flatten_dict(nested)
        print(f"  nested : {nested}")
        print(f"  flat   : {flat}")


def main(spark: SparkSession) -> None:
    demo_row_to_dict(spark)
    demo_dict_to_row(spark)
    demo_row_to_tuple_and_list(spark)
    demo_row_to_named_dict(spark)
    demo_collect_to_dicts(spark)
    demo_dicts_to_dataframe(spark)
    demo_row_to_json(spark)
    demo_normalise_nulls(spark)
    demo_flatten_nested(spark)


if __name__ == "__main__":
    spark = get_spark("row-conversion")
    main(spark)
    spark.stop()
