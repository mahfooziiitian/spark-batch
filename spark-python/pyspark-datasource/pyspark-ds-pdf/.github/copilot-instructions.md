# Copilot Instructions — pyspark-ds-pdf

This project demonstrates how to read **PDF files** directly into **Spark DataFrames**
using the [`spark-pdf`](https://stabrise.com/spark-pdf/) custom data source by StabRise.
Every example is self-contained and runnable locally — no cluster required.

## Modular Instruction Files

| File | Scope (`applyTo`) | Purpose |
|------|--------------------|---------|
| `instructions/python.instructions.md` | `**/*.py` | Python style, imports, type hints, docstrings |
| `instructions/pyspark-pdf.instructions.md` | `src/**/*.py` | spark-pdf datasource read patterns and options |
| `instructions/testing.instructions.md` | `tests/**/*.py` | pytest conventions, SparkSession fixture, assertions |
| `instructions/project-config.instructions.md` | `pyproject.toml`, `uv.lock`, `.python-version` | Package manager and project metadata |
| `instructions/fixtures.instructions.md` | `tests/fixtures/**` | PDF fixture generation with fpdf2 |

## Project Overview

The `spark-pdf` library extends Apache Spark's Data Source API so PDFs are treated
as a native data source. Each page becomes one DataFrame row. For text-based PDFs
the text layer is extracted directly; for scanned PDFs the built-in Tesseract OCR
engine processes the rendered page image.

This project covers:

- Basic PDF reading into a Spark DataFrame (`spark.read.format("pdf")`)
- Key options: `imageType`, `resolution`, `pagePerPartition`, `reader`
- Text extraction from text-layer PDFs
- Multi-file loading with glob patterns
- Saving pages to Parquet for downstream analytics
- Per-page image and OCR access via the `image` and `document` columns
- Fixture generation with `fpdf2` (text articles, invoices, sales reports)

## Technology Stack

| Component | Version / Tool |
|-----------|---------------|
| Python | ≥ 3.11 |
| PySpark | 3.5.x |
| spark-pdf | `com.stabrise:spark-pdf-spark35_2.12:0.1.16` |
| Package manager | uv |
| Testing | pytest ≥ 8.0 |
| Fixture generation | fpdf2 ≥ 2.8 |

## Project Structure

```
pyspark-ds-pdf/
├── .github/
│   ├── copilot-instructions.md          ← you are here
│   └── instructions/
│       ├── python.instructions.md
│       ├── pyspark-pdf.instructions.md
│       ├── testing.instructions.md
│       ├── project-config.instructions.md
│       └── fixtures.instructions.md
├── src/
│   └── pdf/
│       ├── __init__.py
│       ├── pdf_reader.py                # create_spark_session(), read_pdf()
│       └── examples/
│           ├── __init__.py
│           ├── example_text_extraction.py   # text layer, word count per page
│           ├── example_invoice_extraction.py # single-page invoice, dollar amounts
│           ├── example_sales_report.py      # multi-page report, Parquet output
│           └── example_multi_file.py        # glob loading, pages-per-file count
├── tests/
│   ├── __init__.py
│   ├── fixtures/
│   │   ├── generate_fixtures.py         # fpdf2 PDF generator script
│   │   ├── sample.pdf                   # generic multi-page PDF
│   │   ├── text_article.pdf             # 4-page article with rich text
│   │   ├── invoice.pdf                  # 1-page invoice with a table
│   │   ├── sales_report.pdf             # 3-page regional sales report
│   │   └── multi/
│   │       ├── doc_1.pdf
│   │       ├── doc_2.pdf
│   │       └── doc_3.pdf
│   └── test_pdf_reader.py
├── pyproject.toml
├── README.md
└── uv.lock
```

## Quick Reference

```bash
# Install dependencies
uv sync

# Generate PDF fixtures (required before running tests or examples)
uv run python tests/fixtures/generate_fixtures.py

# Run all tests
uv run pytest

# Run a specific example
uv run python src/pdf/examples/example_text_extraction.py
PDF_PATH=tests/fixtures/invoice.pdf uv run python src/pdf/examples/example_invoice_extraction.py
PDF_DIR=tests/fixtures/multi uv run python src/pdf/examples/example_multi_file.py
```

## DataFrame Output Columns

| Column | Type | Description |
|--------|------|-------------|
| `path` | `StringType` | Absolute path to the PDF file |
| `page_number` | `IntegerType` | Page index (0- or 1-based, depending on version) |
| `text` | `StringType` | Extracted text layer (empty for scanned PDFs) |
| `image` | `BinaryType` | Rendered page image bytes |
| `document` | `StringType` | OCR-extracted text via Tesseract |
| `partition_number` | `IntegerType` | Spark partition index |

## Things to Avoid

- Do **not** use `from pyspark.sql.functions import *` — always `import functions as F`.
- Do **not** omit `spark.stop()` in standalone scripts.
- Do **not** set a high `resolution` (e.g. 300+) in tests — use 100 dpi to keep tests fast.
- Do **not** call `df.collect()` on large PDF DataFrames — use `df.count()` or `df.show()`.
- Do **not** forget the `spark.jars.packages` config — without it the `"pdf"` format is unknown.
- Do **not** commit generated PDF fixtures to version control — re-generate with `generate_fixtures.py`.
- Do **not** use `len(df.collect())` — use `df.count()` for row counts.
