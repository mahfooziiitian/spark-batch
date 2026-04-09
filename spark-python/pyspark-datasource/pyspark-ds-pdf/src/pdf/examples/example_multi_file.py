"""
Example 4 – Multi-File PDF Loading with Glob
=============================================
Loads all PDFs from a directory at once using a glob pattern.
Demonstrates per-file page counts and union of pages across documents.

Run:
    PDF_DIR=tests/fixtures/multi uv run python src/pdf/examples/example_multi_file.py
"""

import os

from pyspark.sql import functions as F

from pdf.pdf_reader import create_spark_session, read_pdf

_DEFAULT_DIR = os.path.join(
    os.path.dirname(__file__), "../../../tests/fixtures/multi"
)
PDF_DIR = os.environ.get("PDF_DIR", _DEFAULT_DIR)


def main() -> None:
    spark = create_spark_session("example-multi-file")
    spark.sparkContext.setLogLevel("WARN")

    glob_path = os.path.join(PDF_DIR, "*.pdf")
    df = read_pdf(spark, glob_path, image_type="BINARY", resolution="100", page_per_partition="2")

    print(f"=== All pages across all files ===")
    df.select(
        F.regexp_extract("path", r"([^/]+)$", 1).alias("filename"),
        "page_number",
        F.substring("text", 1, 120).alias("text_preview"),
    ).orderBy("filename", "page_number").show(truncate=False)

    # Pages per file
    print("=== Page count per file ===")
    (
        df.groupBy(F.regexp_extract("path", r"([^/]+)$", 1).alias("filename"))
        .agg(F.count("page_number").alias("pages"))
        .orderBy("filename")
        .show(truncate=False)
    )

    # Total pages loaded
    print(f"Total pages loaded: {df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
