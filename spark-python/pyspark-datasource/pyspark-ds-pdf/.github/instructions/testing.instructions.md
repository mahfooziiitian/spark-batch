---
applyTo: "tests/**/*.py"
---

# Testing Conventions

## Framework

- pytest ≥ 8.0 (dev dependency).
- Run with `uv run pytest` or `uv run pytest tests/ -v --tb=short`.

## Directory Layout

```
tests/
├── __init__.py
├── fixtures/
│   ├── generate_fixtures.py      # fpdf2 generator — run before test suite
│   ├── sample.pdf
│   ├── text_article.pdf          # 4-page article
│   ├── invoice.pdf               # 1-page invoice
│   ├── sales_report.pdf          # 3-page regional report
│   └── multi/                    # 3 × 1-page docs for glob tests
│       ├── doc_1.pdf
│       ├── doc_2.pdf
│       └── doc_3.pdf
└── test_pdf_reader.py
```

## SparkSession Fixture

Defined directly in the test file (or `conftest.py` if shared across multiple files):

```python
import pytest
from pyspark.sql import SparkSession

_SPARK_PDF_PACKAGE = "com.stabrise:spark-pdf-spark35_2.12:0.1.16"

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("test-suite-pdf")
        .master("local[2]")
        .config("spark.jars.packages", _SPARK_PDF_PACKAGE)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

Key settings:
- `local[2]` — two threads; deterministic and fast.
- `shuffle.partitions=2` — default 200 is wasteful for test data.
- `ui.enabled=false` — skip the Spark Web UI.
- `setLogLevel("ERROR")` — suppress all output except errors.

## Read Helper

Define a `_read()` helper to avoid repeating options in every test:

```python
def _read(spark, path=SAMPLE_PDF, **opts):
    reader = spark.read.format("pdf")
    defaults = {"imageType": "BINARY", "resolution": "100", "pagePerPartition": "2", "reader": "pdfBox"}
    for k, v in {**defaults, **opts}.items():
        reader = reader.option(k, v)
    return reader.load(path)
```

Always use `imageType="BINARY"` and `resolution="100"` in tests for speed.

## Test Organisation

Group tests into classes by fixture or capability area:

```python
class TestPdfDataFrameSchema:    # columns present, correct types
class TestPdfDataFrameContent:   # row counts, null checks, sequential pages
class TestPdfDataFrameOptions:   # imageType, resolution, pagePerPartition
class TestPdfFileIO:             # Parquet write + read round-trip
class TestTextArticlePdf:        # text_article.pdf — 4 pages, rich text
class TestInvoicePdf:            # invoice.pdf — 1 page, dollar amounts
class TestSalesReportPdf:        # sales_report.pdf — 3 pages, region names
class TestMultiFilePdf:          # multi/*.pdf — glob loading, file count
```

## Assertions

Prefer `df.count()` over `len(df.collect())`:

```python
assert df.count() == 4
assert set(df.columns) >= {"path", "page_number", "text", "image", "document", "partition_number"}
```

For single-row assertions, collect minimally:

```python
row = df.filter(F.col("page_number") == 0).first()
assert row["text"] is not None
```

For content checks, use DataFrame filters:

```python
match = df.filter(F.col("text").contains("Europe")).count()
assert match >= 1
```

For schema type checks:

```python
from pyspark.sql.types import IntegerType, LongType, StringType

page_field = next(f for f in df.schema.fields if f.name == "page_number")
assert isinstance(page_field.dataType, (IntegerType, LongType))
```

## File I/O Tests

Use pytest's `tmp_path` fixture — unique per test, cleaned up automatically:

```python
def test_write_and_read_parquet(self, spark, tmp_path):
    out = str(tmp_path / "pages.parquet")
    df = _read(spark).select("path", "page_number", "text")
    df.write.mode("overwrite").parquet(out)
    read_back = spark.read.parquet(out)
    assert read_back.count() == df.count()
    assert set(read_back.columns) == {"path", "page_number", "text"}
```

## Fixture PDF Paths

Reference fixture PDFs using `os.path.join(os.path.dirname(__file__), "fixtures", ...)`:

```python
FIXTURES_DIR     = os.path.join(os.path.dirname(__file__), "fixtures")
SAMPLE_PDF       = os.path.join(FIXTURES_DIR, "sample.pdf")
TEXT_ARTICLE_PDF = os.path.join(FIXTURES_DIR, "text_article.pdf")
INVOICE_PDF      = os.path.join(FIXTURES_DIR, "invoice.pdf")
SALES_REPORT_PDF = os.path.join(FIXTURES_DIR, "sales_report.pdf")
MULTI_PDF_GLOB   = os.path.join(FIXTURES_DIR, "multi", "*.pdf")
```

## Entry Point

Every test file includes a direct-run entry point:

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```

## Naming Conventions

- Test files: `test_<topic>.py`
- Test classes: `Test<CapabilityArea>` or `Test<FixtureName>Pdf`
- Test methods: `test_<what_it_verifies>`
