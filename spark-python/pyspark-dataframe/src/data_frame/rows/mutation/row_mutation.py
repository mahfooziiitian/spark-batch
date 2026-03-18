"""
Row mutation patterns — Row objects are immutable, but you can derive a
modified Row from an existing one using these patterns:

  1. Add a field        — Row(**row.asDict(), new_field=value)
  2. Remove a field     — rebuild Row from subset of fields
  3. Update a field     — overwrite a key in the dict copy
  4. Rename a field     — pop old key, insert new key
  5. Merge two Rows     — combine dicts from two Rows
  6. Deep update nested — rebuild nested Row with changed sub-field
  7. DataFrame API      — withColumn / drop / withColumnRenamed (preferred)
  8. RDD map pipeline   — chain multiple mutations via rdd.map

Row is immutable by design — each "mutation" produces a brand-new Row.
For large-scale transformations prefer the DataFrame API (withColumn, select,
drop) which stays inside the Catalyst optimizer.
"""

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from data_frame.sample_data import customer_orders, employees
from data_frame.spark_utils import get_spark


def demo_add_field(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 1. Add a new field to an existing Row
    # ------------------------------------------------------------------
    original = Row(id=1, name="Alice", salary=90000.0)

    # Unpack existing fields then append the new one
    with_dept = Row(**original.asDict(), department="Engineering")  # (1)
    with_both = Row(**original.asDict(), department="Engineering", active=True)

    print("=== Add field ===")
    print(f"  original    : {original}")
    print(f"  +department : {with_dept}")
    print(f"  +both       : {with_both}")

    # Via rdd.map on a DataFrame
    df = spark.createDataFrame(*employees())
    augmented = df.rdd.map(
        lambda r: Row(**r.asDict(), name_length=len(r["employee_name"]))
    ).toDF()
    augmented.show(truncate=False)


def demo_remove_field(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 2. Remove a field — rebuild from a filtered dict
    # ------------------------------------------------------------------
    original = Row(id=1, name="Alice", salary=90000.0, ssn="123-45-6789")

    # Drop the sensitive field before returning/logging
    safe = Row(**{k: v for k, v in original.asDict().items() if k != "ssn"})

    print("\n=== Remove field ===")
    print(f"  original : {original}")
    print(f"  safe     : {safe}")

    # Via rdd.map — drop department_id from employees
    df = spark.createDataFrame(*employees())
    (
        df.rdd.map(
            lambda r: Row(
                **{k: v for k, v in r.asDict().items() if k != "department_id"}
            )
        )
        .toDF()
        .show(truncate=False)
    )


def demo_update_field(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 3. Update (overwrite) a field value
    # ------------------------------------------------------------------
    original = Row(id=1, name="alice smith", salary=90000.0)

    d = original.asDict()
    d["name"] = d["name"].title()  # "alice smith" → "Alice Smith"
    d["salary"] = round(d["salary"] * 1.1, 2)  # 10 % raise

    updated = Row(**d)
    print("\n=== Update field ===")
    print(f"  original : {original}")
    print(f"  updated  : {updated}")

    # Via rdd.map on DataFrame
    df = spark.createDataFrame(*employees())
    (
        df.rdd.map(
            lambda r: Row(
                **{**r.asDict(), "employee_name": r["employee_name"].upper()}  # (2)
            )
        )
        .toDF()
        .show(truncate=False)
    )


def demo_rename_field(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 4. Rename a field — pop old key, insert new key
    # ------------------------------------------------------------------
    original = Row(emp_id=1, emp_name="Alice", dept_id=10)

    d = original.asDict()
    d["id"] = d.pop("emp_id")
    d["name"] = d.pop("emp_name")
    d["department"] = d.pop("dept_id")
    renamed = Row(**d)

    print("\n=== Rename field ===")
    print(f"  original : {original}")
    print(f"  renamed  : {renamed}")


def demo_merge_rows(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 5. Merge two Rows into one (second dict wins on key collision)
    # ------------------------------------------------------------------
    base = Row(id=1, name="Alice", salary=90000.0)
    extra = Row(id=1, department="Engineering", active=True, level=3)

    merged = Row(**{**base.asDict(), **extra.asDict()})  # (3)

    print("\n=== Merge two Rows ===")
    print(f"  base     : {base}")
    print(f"  extra    : {extra}")
    print(f"  merged   : {merged}")


def demo_update_nested(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 6. Update a sub-field in a nested Row
    # ------------------------------------------------------------------
    address = Row(street="Baker St 221B", city="london", country="UK")
    person = Row(id=1, name="Alice", address=address)

    # Rebuild the nested Row with the updated city
    new_addr = Row(**{**person.address.asDict(), "city": "London"})
    updated = Row(**{**person.asDict(), "address": new_addr})

    print("\n=== Update nested Row ===")
    print(f"  original city : {person.address.city}")
    print(f"  updated  city : {updated.address.city}")


def demo_dataframe_mutations(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 7. DataFrame API equivalents — no RDD needed, fully optimized
    # ------------------------------------------------------------------
    df = spark.createDataFrame(*customer_orders())

    result = (
        df.withColumn(
            "line_total", F.round(F.col("quantity") * F.col("unit_price"), 2)
        )  # add
        .withColumn("product", F.upper(F.col("product")))  # update
        .withColumnRenamed("customer_id", "cust_id")  # rename
        .drop("unit_price")  # remove
    )
    print("\n=== DataFrame API mutations ===")
    result.show(truncate=False)


def demo_rdd_mutation_pipeline(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 8. Chain multiple Row mutations in a single rdd.map pass
    # ------------------------------------------------------------------
    df = spark.createDataFrame(*customer_orders())

    def transform_row(r: Row) -> Row:
        d = r.asDict()
        # Add
        d["line_total"] = round(d["quantity"] * d["unit_price"], 2)
        # Update
        d["product"] = d["product"].title()
        d["status"] = d["status"].upper()
        # Remove
        del d["unit_price"]
        return Row(**d)

    print("\n=== RDD mutation pipeline ===")
    (df.rdd.map(transform_row).toDF().orderBy("order_id").show(truncate=False))


def main(spark: SparkSession) -> None:
    demo_add_field(spark)
    demo_remove_field(spark)
    demo_update_field(spark)
    demo_rename_field(spark)
    demo_merge_rows(spark)
    demo_update_nested(spark)
    demo_dataframe_mutations(spark)
    demo_rdd_mutation_pipeline(spark)


# (1) Row(**existing.asDict(), new_key=value) is the idiomatic way to "add"
#     a field — it creates a new Row without touching the original.
# (2) {**r.asDict(), "field": new_value} is the dict-merge update pattern.
# (3) Second dict wins — id=1 appears once (from extra), not twice.

if __name__ == "__main__":
    spark = get_spark("row-mutation")
    main(spark)
    spark.stop()
