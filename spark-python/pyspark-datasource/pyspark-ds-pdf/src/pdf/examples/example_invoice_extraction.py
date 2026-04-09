"""
Example 2 – Invoice Data Extraction
=====================================
Reads a single-page invoice PDF and demonstrates extracting the raw
text layer for downstream parsing / NLP pipelines.

Run:
    PDF_PATH=tests/fixtures/invoice.pdf uv run python src/pdf/examples/example_invoice_extraction.py
"""

import os

from pyspark.sql import functions as F

from pdf.pdf_reader import create_spark_session, read_pdf

PDF_PATH = os.environ.get(
    "PDF_PATH",
    os.path.join(os.path.dirname(__file__), "../../../tests/fixtures/invoice.pdf"),
)


def main() -> None:
    spark = create_spark_session("example-invoice-extraction")
    spark.sparkContext.setLogLevel("WARN")

    df = read_pdf(spark, PDF_PATH, image_type="GREY", resolution="200")

    print("=== Invoice pages ===")
    df.select("path", "page_number").show(truncate=False)

    # Full text of page 1 (invoice is single-page)
    text = df.filter(F.col("page_number") == df.agg(F.min("page_number")).first()[0]) \
             .select("text") \
             .first()["text"]

    print("=== Extracted text ===")
    print(text)

    # Detect lines containing dollar amounts
    amount_lines = (
        df.select(F.explode(F.split("text", r"\n")).alias("line"))
        .filter(F.col("line").rlike(r"\$[\d,]+\.\d{2}"))
        .select(F.trim("line").alias("amount_line"))
    )

    print("\n=== Lines with amounts ===")
    amount_lines.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
