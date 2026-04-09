import os

from pyspark.sql import DataFrame, SparkSession

# Maven coordinate for spark-pdf (Spark 3.5 / Scala 2.12)
_SPARK_PDF_PACKAGE = "com.stabrise:spark-pdf-spark35_2.12:0.1.16"

# Reasonable defaults – callers can override via options dict
_DEFAULTS = {
    "imageType": "RGB",
    "resolution": "200",
    "pagePerPartition": "2",
    "reader": "pdfBox",
}


def create_spark_session(app_name: str = "SparkPDF") -> SparkSession:
    """Build a local SparkSession with the spark-pdf package on the classpath."""
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", _SPARK_PDF_PACKAGE)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def read_pdf(
    spark: SparkSession,
    path: str,
    *,
    image_type: str = _DEFAULTS["imageType"],
    resolution: str = _DEFAULTS["resolution"],
    page_per_partition: str = _DEFAULTS["pagePerPartition"],
    reader: str = _DEFAULTS["reader"],
) -> DataFrame:
    """
    Read one or more PDF files into a Spark DataFrame.

    Each row represents one page and contains:
      - path            : file path
      - page_number     : 0-based page index
      - text            : text layer (empty for scanned PDFs)
      - image           : rendered page image (binary)
      - document        : OCR-extracted text (Tesseract)
      - partition_number: Spark partition index

    Parameters
    ----------
    spark            : active SparkSession (must include spark-pdf package)
    path             : glob-friendly path to PDF file(s), e.g. "/data/docs/*.pdf"
    image_type       : "RGB" | "GREY" | "BINARY"  (default "RGB")
    resolution       : DPI for page rendering             (default "200")
    page_per_partition: pages bundled per Spark partition  (default "2")
    reader           : "pdfBox" | "gs"                    (default "pdfBox")
    """
    return (
        spark.read.format("pdf")
        .option("imageType", image_type)
        .option("resolution", resolution)
        .option("pagePerPartition", page_per_partition)
        .option("reader", reader)
        .load(path)
    )


if __name__ == "__main__":
    import sys

    pdf_path = os.environ.get("PDF_PATH", sys.argv[1] if len(sys.argv) > 1 else "")
    if not pdf_path:
        print("Usage: PDF_PATH=<path> python pdf_reader.py  OR  python pdf_reader.py <path>")
        sys.exit(1)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df = read_pdf(spark, pdf_path)
    print(f"Schema:\n")
    df.printSchema()

    print(f"\nTotal pages: {df.count()}")
    df.select("path", "page_number", "text").show(truncate=80)

    spark.stop()
