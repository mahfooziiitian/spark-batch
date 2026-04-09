---
applyTo: "src/**/*.py"
---

# PySpark PDF Datasource Patterns

## Maven Package

The spark-pdf library is delivered as a JVM package resolved at Spark startup.
Always set `spark.jars.packages` — without it the `"pdf"` format is unknown:

```python
_SPARK_PDF_PACKAGE = "com.stabrise:spark-pdf-spark35_2.12:0.1.16"
```

| Spark version | Maven coordinate |
|---------------|-----------------|
| Spark 3.5 | `com.stabrise:spark-pdf-spark35_2.12:0.1.16` |
| Spark 3.4 | `com.stabrise:spark-pdf-spark34_2.12:0.1.11` |
| Spark 3.3 | `com.stabrise:spark-pdf-spark33_2.12:0.1.16` |
| Spark 4.0 | `com.stabrise:spark-pdf-spark40_2.13:0.1.16` |

## SparkSession

Every standalone script creates a session with the spark-pdf package on the classpath:

```python
import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("pdf-descriptive-name")
    .master(os.environ.get("SPARK_MASTER", "local[*]"))
    .config("spark.jars.packages", "com.stabrise:spark-pdf-spark35_2.12:0.1.16")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
```

Always call `spark.stop()` at the end of standalone scripts.

## Reading PDF Files

### Basic Read

```python
df = spark.read.format("pdf").load("/path/to/document.pdf")
```

### Read Options Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `imageType` | `str` | `"RGB"` | Rendered image colour mode: `"RGB"`, `"GREY"`, `"BINARY"` |
| `resolution` | `str` | `"300"` | DPI for page rendering. Lower = faster; higher = sharper OCR. |
| `pagePerPartition` | `str` | `"5"` | Number of pages bundled into one Spark partition |
| `reader` | `str` | `"pdfBox"` | PDF backend: `"pdfBox"` (pure Java) or `"gs"` (Ghostscript) |

### Option Chaining Style

```python
df = (
    spark.read.format("pdf")
    .option("imageType", "GREY")
    .option("resolution", "200")
    .option("pagePerPartition", "2")
    .option("reader", "pdfBox")
    .load("/path/to/file.pdf")
)
```

### Multi-File and Glob Loading

```python
# single file
spark.read.format("pdf").load("/data/doc.pdf")

# glob — all PDFs in a directory
spark.read.format("pdf").load("/data/docs/*.pdf")

# entire directory
spark.read.format("pdf").load("/data/docs/")
```

## DataFrame Output Columns

```
root
 |-- path:             string (nullable = true)
 |-- page_number:      integer (nullable = true)
 |-- text:             string (nullable = true)   ← text layer (empty for scanned PDFs)
 |-- image:            binary (nullable = true)   ← rendered page image bytes
 |-- document:         string (nullable = true)   ← OCR text via Tesseract
 |-- partition_number: integer (nullable = true)
```

## Text Extraction Pattern

For text-based PDFs the `text` column contains the native text layer.
For scanned PDFs use `document` (requires Tesseract):

```python
from pyspark.sql import functions as F

# native text layer — fast, no OCR required
df.select("page_number", F.substring("text", 1, 200).alias("preview")).show(truncate=False)

# OCR text — only for scanned/image PDFs
df.select("page_number", "document").show(truncate=False)
```

## Word Count per Page

```python
word_counts = df.select(
    "page_number",
    F.size(F.split(F.trim(F.col("text")), r"\s+")).alias("word_count"),
).orderBy("page_number")
```

## Extracting Lines Matching a Pattern

```python
amount_lines = (
    df.select(F.explode(F.split("text", r"\n")).alias("line"))
    .filter(F.col("line").rlike(r"\$[\d,]+\.\d{2}"))
    .select(F.trim("line").alias("amount_line"))
)
```

## Tracking the Source File

```python
df.select(
    F.regexp_extract("path", r"([^/]+)$", 1).alias("filename"),
    "page_number",
    "text",
)
```

## Pages per File

```python
df.groupBy(F.regexp_extract("path", r"([^/]+)$", 1).alias("filename")) \
  .agg(F.count("page_number").alias("pages")) \
  .orderBy("filename") \
  .show()
```

## Saving Pages to Parquet

Parquet is the preferred output format for downstream analytics:

```python
# flat write
df.select("path", "page_number", "text") \
  .write.mode("overwrite") \
  .parquet("/output/pdf_pages")

# partitioned by page number
df.select("path", "page_number", "text") \
  .write.mode("overwrite") \
  .partitionBy("page_number") \
  .parquet("/output/pdf_pages_partitioned")
```

## Resolution vs Performance Trade-off

| Resolution | Use case |
|-----------|----------|
| `72`–`100` | Tests and CI — fast, small images |
| `150`–`200` | General text extraction |
| `300` | High-quality OCR / image capture |

Always use `resolution="100"` (or lower) in tests to keep the suite fast.

## imageType Selection

| `imageType` | When to use |
|------------|-------------|
| `"RGB"` | Default; full colour page images |
| `"GREY"` | Greyscale — smaller images, sufficient for most OCR |
| `"BINARY"` | Black-and-white — smallest images, fastest for tests |

## Example: create_spark_session + read_pdf wrapper

```python
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
    image_type: str = "RGB",
    resolution: str = "200",
    page_per_partition: str = "2",
    reader: str = "pdfBox",
) -> DataFrame:
    """Load one or more PDF files into a Spark DataFrame (one row per page)."""
    return (
        spark.read.format("pdf")
        .option("imageType", image_type)
        .option("resolution", resolution)
        .option("pagePerPartition", page_per_partition)
        .option("reader", reader)
        .load(path)
    )
```
