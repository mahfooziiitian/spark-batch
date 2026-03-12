# Getting Started

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.11+ |
| Java JDK | 8 or 17 |
| Apache Spark | 3.x (PySpark < 4.0.0) |
| Package manager | [uv](https://docs.astral.sh/uv/) |

---

## Installation

```bash
# Clone and install
cd spark-xml
uv sync
```

This installs all dependencies defined in `pyproject.toml`:

- **pyspark** < 4.0.0
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

## spark-xml JAR

The [Databricks spark-xml](https://github.com/databricks/spark-xml) library is loaded as a Maven package. There are two approaches:

=== "Automatic (Spark 3.4+)"

    Use the short `"xml"` format alias — Spark resolves the JAR automatically if pre-installed:

    ```python
    df = spark.read.format("xml").option("rowTag", "person").load(path)
    ```

=== "Explicit Maven Package"

    Configure the JAR in SparkSession builder:

    ```python
    spark = (
        SparkSession.builder
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0")
        .getOrCreate()
    )
    ```

=== "Session Utility"

    Use the project's helper function:

    ```python
    from spark_xml.util.session.spark_session_util import get_spark_session

    spark = get_spark_session(
        app_name="my-app",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )
    ```

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
uv run python src/spark_xml/compression/spark_xml_gzip.py
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
