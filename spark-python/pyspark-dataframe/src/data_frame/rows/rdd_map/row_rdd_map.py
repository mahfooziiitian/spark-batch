"""
Row processing via the RDD API — map, flatMap, filter, mapPartitions.

The RDD layer gives full Python control over each row but bypasses the
Catalyst optimizer. Prefer DataFrame API / Spark SQL for transformations
that can be expressed with built-in functions.

Use df.rdd when you need:
  • Custom Python logic not expressible in Spark SQL
  • Row restructuring that changes the schema
  • flatMap — one input row → zero or more output rows

Patterns covered:
  1. map           — transform each Row, return new Row
  2. filter        — keep rows matching a predicate
  3. flatMap       — expand one row into multiple rows
  4. mapPartitions — batch processing per partition
  5. map → toDF    — convert transformed RDD back to DataFrame
  6. map + schema  — createDataFrame(rdd, schema) for typed output
  7. keyBy / groupBy on RDD rows
"""

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from data_frame.sample_data import customer_orders, employees, product_revenue
from data_frame.spark_utils import get_spark


def demo_rdd_map(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees())

    # ------------------------------------------------------------------
    # 1. map — transform each Row, return a new Row
    # ------------------------------------------------------------------
    enriched_rdd = df.rdd.map(
        lambda r: Row(
            id=r["id"],
            employee_name=r["employee_name"].upper(),  # uppercase name
            department_id=r["department_id"],
            name_length=len(r["employee_name"]),  # derived field
        )
    )
    print("=== rdd.map — enrich rows ===")
    spark.createDataFrame(enriched_rdd).show(truncate=False)


def demo_rdd_filter(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    # ------------------------------------------------------------------
    # 2. filter — keep rows where the predicate returns True
    # ------------------------------------------------------------------
    active_rdd = df.rdd.filter(
        lambda r: r["status"] == "active" and r["customer_id"] is not None
    )
    high_value_rdd = active_rdd.filter(
        lambda r: r["quantity"] * r["unit_price"] >= 50.0
    )
    print("=== rdd.filter — active high-value orders ===")
    spark.createDataFrame(high_value_rdd).show(truncate=False)


def demo_rdd_flatmap(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 3. flatMap — one input row → zero or more output rows
    # Use when a single source record represents multiple logical events
    # ------------------------------------------------------------------
    data = [
        Row(product="Widget", tags="electronics,popular,sale"),
        Row(product="Gadget", tags="electronics,new"),
        Row(product="Book", tags="education"),
    ]
    rdd = spark.sparkContext.parallelize(data)

    # Split each tag string into separate (product, tag) rows
    tag_rdd = rdd.flatMap(
        lambda r: [
            Row(product=r["product"], tag=tag.strip()) for tag in r["tags"].split(",")
        ]
    )
    print("=== rdd.flatMap — one row per tag ===")
    spark.createDataFrame(tag_rdd).show(truncate=False)


def demo_rdd_map_partitions(spark: SparkSession) -> None:
    df = spark.createDataFrame(*product_revenue()).repartition(2)

    # ------------------------------------------------------------------
    # 4. mapPartitions — process all rows in a partition together
    # Useful when setup cost per partition is high (e.g. opening a file,
    # connecting to a database, loading a model)
    # ------------------------------------------------------------------
    def apply_discount(rows):
        # "Set up" once per partition (e.g. load a lookup table)
        discount_rates = {"Electronics": 0.1, "Apparel": 0.15, "Books": 0.05}

        for row in rows:
            rate = discount_rates.get(row["category"], 0.0)
            yield Row(
                product=row["product"],
                category=row["category"],
                original_revenue=row["revenue"],
                discount=round(row["revenue"] * rate, 2),
                final_revenue=round(row["revenue"] * (1 - rate), 2),
            )

    print("=== rdd.mapPartitions ===")
    spark.createDataFrame(df.rdd.mapPartitions(apply_discount)).orderBy(
        "category", "product"
    ).show(truncate=False)


def demo_map_to_df(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees())

    # ------------------------------------------------------------------
    # 5. map → toDF() — infer schema from the first returned Row
    # Named-keyword Rows are required for schema inference to work
    # ------------------------------------------------------------------
    result_df = df.rdd.map(
        lambda r: Row(
            id=r["id"],
            full_label=f"[{r['id']:02d}] {r['employee_name']}",
        )
    ).toDF()  # schema from Row field names
    print("=== rdd.map → toDF() ===")
    result_df.show(truncate=False)


def demo_map_with_explicit_schema(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    # ------------------------------------------------------------------
    # 6. map + explicit schema — createDataFrame(rdd, schema)
    # Preferred over toDF() when the output schema must be exact
    # ------------------------------------------------------------------
    out_schema = StructType(
        [
            StructField("order_id", IntegerType(), nullable=False),
            StructField("product", StringType(), nullable=True),
            StructField("line_total", DoubleType(), nullable=True),
            StructField("vat", DoubleType(), nullable=True),
        ]
    )

    rdd = df.rdd.map(
        lambda r: Row(
            order_id=r["order_id"],
            product=r["product"],
            line_total=round(r["quantity"] * r["unit_price"], 2),
            vat=round(r["quantity"] * r["unit_price"] * 0.2, 2),
        )
    )
    print("=== createDataFrame(rdd, schema) ===")
    spark.createDataFrame(rdd, out_schema).show(truncate=False)


def demo_key_by(spark: SparkSession) -> None:
    df = spark.createDataFrame(*product_revenue())

    # ------------------------------------------------------------------
    # 7. keyBy — create (key, Row) pairs for groupBy-style RDD ops
    # ------------------------------------------------------------------
    by_category = (
        df.rdd.keyBy(lambda r: r["category"])  # (category, Row) pairs
        .groupByKey()
        .mapValues(lambda rows: sorted([r["product"] for r in rows]))
        .collect()
    )
    print("=== rdd.keyBy + groupByKey ===")
    for category, products in sorted(by_category):
        print(f"  {category:15s}: {products}")


def main(spark: SparkSession) -> None:
    demo_rdd_map(spark)
    demo_rdd_filter(spark)
    demo_rdd_flatmap(spark)
    demo_rdd_map_partitions(spark)
    demo_map_to_df(spark)
    demo_map_with_explicit_schema(spark)
    demo_key_by(spark)


if __name__ == "__main__":
    spark = get_spark("row-rdd-map")
    main(spark)
    spark.stop()
