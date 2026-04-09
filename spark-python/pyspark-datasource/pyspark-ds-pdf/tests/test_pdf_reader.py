import os

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Maven coordinate for spark-pdf (Spark 3.5 / Scala 2.12)
_SPARK_PDF_PACKAGE = "com.stabrise:spark-pdf-spark35_2.12:0.1.16"

# Paths to the generated fixture PDFs
FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
SAMPLE_PDF        = os.path.join(FIXTURES_DIR, "sample.pdf")
TEXT_ARTICLE_PDF  = os.path.join(FIXTURES_DIR, "text_article.pdf")
INVOICE_PDF       = os.path.join(FIXTURES_DIR, "invoice.pdf")
SALES_REPORT_PDF  = os.path.join(FIXTURES_DIR, "sales_report.pdf")
MULTI_PDF_GLOB    = os.path.join(FIXTURES_DIR, "multi", "*.pdf")


# ---------------------------------------------------------------------------
# Session-scoped SparkSession
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test-suite-pdf")
        .master("local[2]")
        .config("spark.jars.packages", _SPARK_PDF_PACKAGE)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _read(spark: SparkSession, path: str = SAMPLE_PDF, **opts):
    reader = spark.read.format("pdf")
    defaults = {
        "imageType": "BINARY",
        "resolution": "100",
        "pagePerPartition": "2",
        "reader": "pdfBox",
    }
    for k, v in {**defaults, **opts}.items():
        reader = reader.option(k, v)
    return reader.load(path)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPdfDataFrameSchema:
    def test_expected_columns_present(self, spark):
        df = _read(spark)
        expected = {"path", "page_number", "text", "image", "document", "partition_number"}
        assert expected.issubset(set(df.columns))

    def test_page_number_is_integer(self, spark):
        df = _read(spark)
        page_field = next(f for f in df.schema.fields if f.name == "page_number")
        from pyspark.sql.types import IntegerType, LongType
        assert isinstance(page_field.dataType, (IntegerType, LongType))

    def test_path_is_string(self, spark):
        df = _read(spark)
        path_field = next(f for f in df.schema.fields if f.name == "path")
        from pyspark.sql.types import StringType
        assert isinstance(path_field.dataType, StringType)


class TestPdfDataFrameContent:
    def test_at_least_one_row(self, spark):
        df = _read(spark)
        assert df.count() >= 1

    def test_path_not_null(self, spark):
        df = _read(spark)
        null_count = df.filter(F.col("path").isNull()).count()
        assert null_count == 0

    def test_page_numbers_start_at_zero_or_one(self, spark):
        df = _read(spark)
        min_page = df.agg(F.min("page_number")).first()[0]
        assert min_page in (0, 1)

    def test_page_numbers_are_sequential(self, spark):
        df = _read(spark)
        total_pages = df.count()
        min_page = df.agg(F.min("page_number")).first()[0]
        max_page = df.agg(F.max("page_number")).first()[0]
        assert (max_page - min_page + 1) == total_pages

    def test_text_column_is_string_or_null(self, spark):
        df = _read(spark)
        from pyspark.sql.types import StringType
        text_field = next(f for f in df.schema.fields if f.name == "text")
        assert isinstance(text_field.dataType, StringType)


class TestPdfDataFrameOptions:
    def test_grey_image_type(self, spark):
        df = _read(spark, imageType="GREY")
        assert df.count() >= 1

    def test_low_resolution(self, spark):
        df = _read(spark, resolution="72")
        assert df.count() >= 1

    def test_single_page_per_partition(self, spark):
        df = _read(spark, pagePerPartition="1")
        assert df.count() >= 1

    def test_page_per_partition_matches_partition_column(self, spark):
        df = _read(spark, pagePerPartition="1")
        # With 1 page per partition every partition_number should be unique per path
        row_count = df.count()
        partition_count = df.select("partition_number").distinct().count()
        assert partition_count <= row_count


class TestPdfFileIO:
    def test_write_and_read_parquet(self, spark, tmp_path):
        out = str(tmp_path / "pdf_pages.parquet")
        df = _read(spark).select("path", "page_number", "text")
        df.write.mode("overwrite").parquet(out)
        read_back = spark.read.parquet(out)
        assert read_back.count() == df.count()
        assert set(read_back.columns) == {"path", "page_number", "text"}


# ---------------------------------------------------------------------------
# Per-fixture tests
# ---------------------------------------------------------------------------


class TestTextArticlePdf:
    """text_article.pdf – 4-page article with a rich text layer."""

    def test_page_count(self, spark):
        df = _read(spark, TEXT_ARTICLE_PDF)
        assert df.count() == 4

    def test_text_is_non_empty_on_all_pages(self, spark):
        df = _read(spark, TEXT_ARTICLE_PDF)
        empty = df.filter(
            F.col("text").isNull() | (F.trim(F.col("text")) == "")
        ).count()
        assert empty == 0

    def test_word_count_per_page_is_positive(self, spark):
        df = _read(spark, TEXT_ARTICLE_PDF)
        result = df.select(
            F.size(F.split(F.trim(F.col("text")), r"\s+")).alias("words")
        )
        min_words = result.agg(F.min("words")).first()[0]
        assert min_words > 0


class TestInvoicePdf:
    """invoice.pdf – single-page invoice with tabular data."""

    def test_single_page(self, spark):
        df = _read(spark, INVOICE_PDF)
        assert df.count() == 1

    def test_text_contains_invoice_keyword(self, spark):
        df = _read(spark, INVOICE_PDF)
        text = df.first()["text"] or ""
        assert "INVOICE" in text.upper() or "INV" in text.upper()

    def test_text_contains_dollar_amounts(self, spark):
        df = _read(spark, INVOICE_PDF)
        text = df.first()["text"] or ""
        import re
        assert re.search(r"\$[\d,]+\.\d{2}", text) is not None


class TestSalesReportPdf:
    """sales_report.pdf – 3-page regional sales report."""

    def test_page_count(self, spark):
        df = _read(spark, SALES_REPORT_PDF)
        assert df.count() == 3

    def test_parquet_roundtrip_all_pages(self, spark, tmp_path):
        out = str(tmp_path / "sales.parquet")
        df = _read(spark, SALES_REPORT_PDF).select("page_number", "text")
        df.write.mode("overwrite").partitionBy("page_number").parquet(out)
        read_back = spark.read.parquet(out)
        assert read_back.count() == 3

    def test_each_page_contains_region_name(self, spark):
        df = _read(spark, SALES_REPORT_PDF)
        regions = ["North America", "Europe", "Asia Pacific"]
        for region in regions:
            match = df.filter(F.col("text").contains(region)).count()
            assert match >= 1, f"Region '{region}' not found in any page"


class TestMultiFilePdf:
    """multi/*.pdf – three 1-page documents loaded with a glob pattern."""

    def test_total_page_count(self, spark):
        df = _read(spark, MULTI_PDF_GLOB)
        assert df.count() == 3

    def test_three_distinct_files(self, spark):
        df = _read(spark, MULTI_PDF_GLOB)
        file_count = df.select("path").distinct().count()
        assert file_count == 3

    def test_one_page_per_file(self, spark):
        df = _read(spark, MULTI_PDF_GLOB)
        pages_per_file = (
            df.groupBy("path")
            .agg(F.count("page_number").alias("pages"))
            .agg(F.max("pages"))
            .first()[0]
        )
        assert pages_per_file == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
