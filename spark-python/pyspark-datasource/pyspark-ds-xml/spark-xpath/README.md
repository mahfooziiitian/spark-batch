# Spark XPath

[![CI](https://github.com/<owner>/<repo>/actions/workflows/ci.yml/badge.svg)](https://github.com/<owner>/<repo>/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![PySpark < 4.0](https://img.shields.io/badge/pyspark-%3C4.0-orange.svg)](https://spark.apache.org/)

Extract and transform XML data inside PySpark DataFrames using built-in
XPath functions (`xpath_string`, `xpath_boolean`, `xpath`, …).

---

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Running Tests](#running-tests)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

---

## Features

| Capability | Description |
|---|---|
| **xpath_string** | Extract a single string value from XML |
| **xpath_boolean** | Evaluate a boolean XPath expression |
| **xpath (array)** | Return an array of matching text nodes |
| **Nested XML** | Parse deeply nested / namespaced XML documents |
| **Spark SQL** | All examples work as Spark SQL expressions in `spark.sql()` |

---

## Prerequisites

- Python **≥ 3.11**
- Java **8 / 11 / 17** (required by Spark)
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

---

## Installation

```bash
# Clone the repository
git clone https://github.com/<owner>/<repo>.git
cd spark-xpath

# Create virtual environment & install dependencies
uv sync            # installs runtime + dev deps
# — or —
pip install -e ".[dev]"
```

---

## Quick Start

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

spark = SparkSession.builder.master("local[*]").appName("xpath-demo").getOrCreate()

xml = ["<Msg><Header><tag1>hello</tag1></Header></Msg>"]
df = spark.createDataFrame(xml, StringType()).withColumnRenamed("value", "data")
df.createOrReplaceTempView("xml_data")

spark.sql("""
    SELECT xpath_string(data, 'Msg/Header/tag1') AS tag1
    FROM xml_data
""").show()
# +-----+
# | tag1|
# +-----+
# |hello|
# +-----+
```

See [docs/examples](docs/examples/) for more complete scenarios.

---

## Project Structure

```
spark-xpath/
├── .github/
│   └── workflows/
│       └── ci.yml              # GitHub Actions CI pipeline
├── docs/                       # MkDocs documentation source
│   ├── index.md
│   ├── getting-started.md
│   ├── xpath-functions.md
│   ├── testing.md
│   └── examples/
│       ├── basic-parsing.md
│       ├── nested-xml.md
│       └── credit-evaluation.md
├── src/
│   └── xpath/
│       ├── __init__.py
│       ├── xml_data_parsing.py      # Basic XML ➜ DataFrame
│       ├── xml_xpath.py             # Credit-evaluation XPath demo
│       ├── python_check.py          # Utility snippets
│       ├── text/
│       │   ├── __init__.py
│       │   └── xml_xpath_text.py    # xpath() array extraction
│       └── nested/
│           ├── __init__.py
│           └── nested_xml_xpath.py  # File-based nested XML
├── tests/
│   └── xml/
│       ├── __init__.py
│       └── test_xml_array_xpath.py  # Pytest suite
├── mkdocs.yml
├── pyproject.toml
├── CONTRIBUTING.md
└── README.md
```

---

## Running Tests

```bash
# Using uv (recommended)
uv run pytest tests/ -v

# Using plain pytest
pytest tests/ -v
```

---

## Documentation

Documentation is built with [MkDocs](https://www.mkdocs.org/) + [Material theme](https://squidfundez.github.io/mkdocs-material/).

```bash
# Serve locally (hot-reload)
uv run mkdocs serve

# Build static site
uv run mkdocs build
```

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to contribute.

---

## License

This project is provided as-is for educational and internal use.