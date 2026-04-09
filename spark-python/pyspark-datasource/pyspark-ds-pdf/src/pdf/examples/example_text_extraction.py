"""
Example 1 – Text Extraction from a Text-Based PDF
==================================================
Reads a multi-page article PDF into a Spark DataFrame and extracts
the text layer from each page.

Run:
    PDF_PATH=tests/fixtures/text_article.pdf uv run python src/pdf/examples/example_text_extraction.py
"""

import os

from pyspark.sql import functions as F

from pdf.pdf_reader import create_spark_session, read_pdf

PDF_PATH = os.environ.get(
    "PDF_PATH",
    os.path.join(os.path.dirname(__file__), "../../../tests/fixtures/text_article.pdf"),
)


def main() -> None:
    spark = create_spark_session("example-text-extraction")
    spark.sparkContext.setLogLevel("WARN")

    df = read_pdf(spark, PDF_PATH, image_type="RGB", resolution="150")

    print("=== Schema ===")
    df.printSchema()

    print(f"\n=== Total pages: {df.count()} ===\n")

    # Show page number and a snippet of the extracted text
    df.select(
        "page_number",
        F.substring("text", 1, 200).alias("text_preview"),
    ).orderBy("page_number").show(truncate=False)

    # Word count per page
    word_counts = df.select(
        "page_number",
        F.size(F.split(F.trim(F.col("text")), r"\s+")).alias("word_count"),
    ).orderBy("page_number")

    print("=== Word count per page ===")
    word_counts.show()

    spark.stop()


if __name__ == "__main__":
    main()
