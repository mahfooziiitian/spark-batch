"""
Example 3 – Sales Report: Multi-Page Aggregations
==================================================
Reads a 3-page regional sales report PDF.  Each page covers one region.
Demonstrates per-page text extraction and saving pages to Parquet for
downstream analytics.

Run:
    PDF_PATH=tests/fixtures/sales_report.pdf uv run python src/pdf/examples/example_sales_report.py
"""

import os

from pyspark.sql import functions as F

from pdf.pdf_reader import create_spark_session, read_pdf

PDF_PATH = os.environ.get(
    "PDF_PATH",
    os.path.join(os.path.dirname(__file__), "../../../tests/fixtures/sales_report.pdf"),
)
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/sales_report_pages")


def main() -> None:
    spark = create_spark_session("example-sales-report")
    spark.sparkContext.setLogLevel("WARN")

    df = read_pdf(spark, PDF_PATH, image_type="RGB", resolution="150", page_per_partition="1")

    print(f"=== Total pages: {df.count()} ===\n")

    # Extract a heading preview from each page (first 100 chars of text)
    df.select(
        "page_number",
        F.substring("text", 1, 100).alias("heading_preview"),
    ).orderBy("page_number").show(truncate=False)

    # Persist text pages to Parquet
    (
        df.select("path", "page_number", "text")
        .write.mode("overwrite")
        .partitionBy("page_number")
        .parquet(OUTPUT_PATH)
    )
    print(f"\n=== Pages written to Parquet: {OUTPUT_PATH} ===")

    read_back = spark.read.parquet(OUTPUT_PATH)
    print(f"Rows read back: {read_back.count()}")
    read_back.orderBy("page_number").select("page_number", F.substring("text", 1, 80).alias("text")).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
