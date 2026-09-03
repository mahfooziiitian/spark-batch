# Copilot Instructions — pyspark-ds-xml

This directory contains **three sub-projects** demonstrating different
approaches to XML processing with PySpark. Each sub-project has its own
copilot instructions.

## Sub-Projects

| Sub-Project | Approach | Key Dependency |
|-------------|----------|----------------|
| [`spark-xml`](spark-xml/) | Databricks spark-xml JAR | not needed `com.databricks:spark-xml_2.12:0.18.0` |
| [`spark-xml-etree`](spark-xml-etree/) | Python `xml.etree.ElementTree` via Spark UDFs | None (stdlib) |
| [`spark-xpath`](spark-xpath/) | Spark built-in XPath SQL functions | None (built-in) |

## When to Use Each Approach

- **spark-xml** — Best for reading/writing entire XML files as DataFrames.
  Handles nested XML, attributes, and namespaces. Requires the Databricks
  spark-xml JAR.
- **spark-xml-etree** — Best when you need fine-grained control over XML
  parsing logic using Python's standard library. No external JARs required.
  Parsing runs in Python UDFs (slower than JVM-based parsing).
- **spark-xpath** — Best for extracting specific values from XML columns
  already loaded into a DataFrame. Uses Spark's built-in `xpath_*` SQL
  functions. No external dependencies.

## Copilot Instructions per Sub-Project

Each sub-project contains its own `.github/copilot-instructions.md` with
detailed patterns and conventions:

- `spark-xml/.github/copilot-instructions.md`
- `spark-xml-etree/.github/copilot-instructions.md`
- `spark-xpath/.github/copilot-instructions.md`

Refer to the sub-project instructions for datasource-specific guidance.

## Technology Stack (shared)

| Component | Version / Tool |
|-----------|---------------|
| Python | ≥ 3.11 |
| PySpark | 4.x.x |
| Package manager | uv (preferred) |
| Testing | pytest ≥ 8.0 |

## Conventions

- Use `SPARK_MASTER` env var with `local[*]` fallback.
- `from pyspark.sql import functions as F` — never `import *`.
- Always call `spark.stop()` at the end of standalone scripts.

## Things to Avoid

- Do not use `from pyspark.sql.functions import *`.
- Do not omit `spark.stop()` in standalone scripts.
- Do not use `len(df.collect())` — use `df.count()`.
