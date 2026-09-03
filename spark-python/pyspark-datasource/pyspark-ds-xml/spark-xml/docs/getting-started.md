# Getting Started

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.11+ |
| Java JDK | 17 or 21 |
| Apache Spark | 4.0+ (PySpark >= 4.0.0) |
| Package manager | [uv](https://docs.astral.sh/uv/) |

---

## Installation

```bash
# Clone and install
cd spark-xml
uv sync
```

This installs all dependencies defined in `pyproject.toml`:

- **pyspark** >= 4.0.0 (built-in XML data source)
- **xmlschema** — XSD validation
- **faker** — test data generation
- **chardet** — encoding detection
- **pandas** — alternative XML reading
- **requests** — HTTP XML fetching

---

## Environment Variables

Set these before running any script:

```bash
export JAVA_HOME_17=/usr/lib/jvm/java-17-openjdk   # or your JDK 17 path
export JAVA_HOME_8=/usr/lib/jvm/java-8-openjdk     # optional, for legacy scripts
export DATA_HOME=~/data                              # root for XML/XSD data files
export SPARK_WAREHOUSE=/tmp/spark-warehouse          # for SQL examples
```

Every script sets `JAVA_HOME` and `PYSPARK_PYTHON` automatically:

```python
import os, sys
os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable
```

---

## Built-in XML data source

Since **Spark 4.0**, the XML data source is **built into Apache Spark** — no
external JAR or `spark.jars.packages` configuration is required. Use the short
`"xml"` format alias for reading and writing:

```python
df = spark.read.format("xml").option("rowTag", "person").load(path)

df.write.format("xml").option("rootTag", "people").option("rowTag", "person").save(out)
```

Parsing an XML string **column** uses the built-in `from_xml` / `schema_of_xml`
functions:

```python
from pyspark.sql.functions import from_xml, schema_of_xml, lit

options = {"rowTag": "person"}
sample = df.select("content").first()["content"]
schema = df.select(schema_of_xml(lit(sample), options)).first()[0]
parsed = df.withColumn("parsed", from_xml("content", schema, options))
```

!!! note "Migrated from the Databricks spark-xml JAR"
    Earlier Spark 3.x examples used `com.databricks:spark-xml` via
    `spark.jars.packages` and a JVM `from_xml` bridge. On Spark 4 these are no
    longer needed — the `"xml"` format and `from_xml`/`schema_of_xml` functions
    are native.

---

## Data Directory Layout

Scripts expect data files under `DATA_HOME`:

```
$DATA_HOME/
├── file_data/xml/
│   ├── person.xml
│   ├── books.xml
│   ├── movies.xml
│   ├── orders.xml
│   ├── orders.xsd
│   ├── notes.xml
│   ├── notes.xsd
│   ├── book.xml              # namespaced XML
│   ├── pos.xml               # array examples
│   ├── nested_xml.xml
│   ├── encoding/             # encoding test files
│   ├── date_time/            # date/time test files
│   └── schema/               # JSON schema files
└── certificates/             # SSL certs (for API examples)
```

---

## Verify Installation

```bash
# Run a simple compression example
uv run python examples/compression/spark_xml_gzip.py
```

If you see a DataFrame printed with `Name` and `Age` columns, everything is working.

---

## Development Tools

```bash
# Lint
uv run black src/ tests/
uv run isort src/ tests/
uv run flake8 src/ tests/

# Test
uv run pytest tests/ -v

# Documentation
uv run mkdocs serve      # http://localhost:8000
uv run mkdocs build      # Build static site to site/
```

```mermaid
flowchart LR
    CODE["Write Code"] --> LINT["black / isort / flake8"]
    LINT --> TEST["pytest"]
    TEST --> DOCS["mkdocs serve"]
    DOCS --> COMMIT["git commit"]
```
