"""XML writer — read, transform, write pipeline.

Demonstrates real-world ETL patterns: read XML → transform → write XML.
Includes filtering, aggregation, joining, and multi-output scenarios.
"""

import os
import sys
import textwrap
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    sum as spark_sum,
    struct,
    when,
)

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

ORDERS_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <orders>
      <order>
        <order_id>ORD-001</order_id>
        <customer>Alice Johnson</customer>
        <region>North</region>
        <product>Laptop</product>
        <quantity>2</quantity>
        <unit_price>1299.99</unit_price>
        <status>completed</status>
      </order>
      <order>
        <order_id>ORD-002</order_id>
        <customer>Bob Smith</customer>
        <region>South</region>
        <product>Phone</product>
        <quantity>5</quantity>
        <unit_price>899.99</unit_price>
        <status>completed</status>
      </order>
      <order>
        <order_id>ORD-003</order_id>
        <customer>Carol Williams</customer>
        <region>North</region>
        <product>Tablet</product>
        <quantity>3</quantity>
        <unit_price>499.99</unit_price>
        <status>pending</status>
      </order>
      <order>
        <order_id>ORD-004</order_id>
        <customer>Dave Brown</customer>
        <region>East</region>
        <product>Laptop</product>
        <quantity>1</quantity>
        <unit_price>1499.99</unit_price>
        <status>cancelled</status>
      </order>
      <order>
        <order_id>ORD-005</order_id>
        <customer>Eve Davis</customer>
        <region>West</region>
        <product>Monitor</product>
        <quantity>4</quantity>
        <unit_price>349.99</unit_price>
        <status>completed</status>
      </order>
      <order>
        <order_id>ORD-006</order_id>
        <customer>Frank Miller</customer>
        <region>North</region>
        <product>Phone</product>
        <quantity>2</quantity>
        <unit_price>799.99</unit_price>
        <status>completed</status>
      </order>
      <order>
        <order_id>ORD-007</order_id>
        <customer>Grace Lee</customer>
        <region>South</region>
        <product>Laptop</product>
        <quantity>1</quantity>
        <unit_price>1399.99</unit_price>
        <status>pending</status>
      </order>
      <order>
        <order_id>ORD-008</order_id>
        <customer>Hank Wilson</customer>
        <region>East</region>
        <product>Tablet</product>
        <quantity>2</quantity>
        <unit_price>549.99</unit_price>
        <status>completed</status>
      </order>
    </orders>
""")


def generate_source_xml(path: Path) -> None:
    """Write the source orders XML file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(ORDERS_XML, encoding="utf-8")
    print(f"Generated source XML → {path}")


if __name__ == "__main__":
    data_dir = Path(os.environ["DATA_HOME"]) / "file_data" / "xml"
    out_dir = data_dir / "writer_output"
    out_dir.mkdir(parents=True, exist_ok=True)

    source_path = data_dir / "pipeline_orders.xml"
    generate_source_xml(source_path)

    spark = get_spark_session(
        app_name="xml-writer-pipeline",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # ── Read source XML ─────────────────────────────────────────────
    df = (
        spark.read.format("xml")
        .option("rowTag", "order")
        .load(source_path.as_posix())
    )
    print("=== Source Orders ===")
    df.printSchema()
    df.show(truncate=False)

    # ── 1. Filter and write — completed orders only ─────────────────
    print("\n=== 1. Filter: completed orders only ===")
    completed_path = (out_dir / "completed_orders").as_posix()
    df_completed = df.filter(col("status") == "completed")
    (
        df_completed.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "completed_orders")
        .option("rowTag", "order")
        .save(completed_path)
    )
    count_c = spark.read.format("xml").option("rowTag", "order").load(completed_path).count()
    print(f"Wrote {count_c} completed orders")

    # ── 2. Enrich and write — add computed columns ──────────────────
    print("\n=== 2. Enrich: add total_amount + priority ===")
    enriched_path = (out_dir / "enriched_orders").as_posix()
    df_enriched = df.select(
        col("order_id"),
        col("customer"),
        col("region"),
        col("product"),
        col("quantity"),
        col("unit_price"),
        spark_round(col("quantity") * col("unit_price"), 2).alias("total_amount"),
        when(col("quantity") * col("unit_price") > 2000, "HIGH")
        .when(col("quantity") * col("unit_price") > 1000, "MEDIUM")
        .otherwise("LOW")
        .alias("priority"),
        col("status"),
    )
    (
        df_enriched.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "enriched_orders")
        .option("rowTag", "order")
        .save(enriched_path)
    )
    spark.read.format("xml").option("rowTag", "order").load(enriched_path).show(truncate=False)

    # ── 3. Aggregate and write — summary by region ──────────────────
    print("\n=== 3. Aggregate: summary by region ===")
    summary_path = (out_dir / "region_summary").as_posix()
    df_summary = (
        df.filter(col("status") == "completed")
        .withColumn("total", spark_round(col("quantity") * col("unit_price"), 2))
        .groupBy("region")
        .agg(
            count("*").alias("order_count"),
            spark_sum("total").alias("total_revenue"),
            spark_round(avg("total"), 2).alias("avg_order_value"),
            spark_min("total").alias("min_order"),
            spark_max("total").alias("max_order"),
        )
    )
    (
        df_summary.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "summary")
        .option("rowTag", "region")
        .save(summary_path)
    )
    spark.read.format("xml").option("rowTag", "region").load(summary_path).show(truncate=False)

    # ── 4. Multi-output — split by status ───────────────────────────
    print("\n=== 4. Multi-output: split by status ===")
    for status in ["completed", "pending", "cancelled"]:
        status_path = (out_dir / f"orders_{status}").as_posix()
        df_status = df.filter(col("status") == status)
        (
            df_status.write.format("xml")
            .mode("overwrite")
            .option("rootTag", f"{status}_orders")
            .option("rowTag", "order")
            .save(status_path)
        )
        cnt = spark.read.format("xml").option("rowTag", "order").load(status_path).count()
        print(f"  {status}: {cnt} orders → {status_path}")

    # ── 5. Restructure — flatten into nested XML ────────────────────
    print("\n=== 5. Restructure: nest flat columns ===")
    restructured_path = (out_dir / "restructured_orders").as_posix()
    df_nested = df.select(
        col("order_id").alias("_id"),
        struct(
            col("customer").alias("name"),
            col("region"),
        ).alias("customer_info"),
        struct(
            col("product"),
            col("quantity"),
            col("unit_price"),
            spark_round(col("quantity") * col("unit_price"), 2).alias("total"),
        ).alias("line_item"),
        col("status"),
    )
    (
        df_nested.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "orders")
        .option("rowTag", "order")
        .option("attributePrefix", "_")
        .save(restructured_path)
    )
    df_restr = (
        spark.read.format("xml")
        .option("rowTag", "order")
        .option("attributePrefix", "_")
        .load(restructured_path)
    )
    df_restr.printSchema()
    df_restr.show(truncate=False)

    spark.stop()
