# GitHub Copilot Instructions — pyspark-pytest

## Project Overview

A **PySpark testing reference project** demonstrating general PySpark testing patterns
using pytest and PySpark's built-in testing utilities (`pyspark.testing`). Includes
data processing pipelines, reader utilities, transformations, and Faker-based test
data generation.

## Tech Stack

| Component        | Version / Tool       |
| ---------------- | -------------------- |
| Python           | ^3.11                |
| PySpark          | ^3.5.0               |
| Testing          | pytest               |
| Assertions       | pyspark.testing      |
| Test data        | Faker ^25.8.0        |
| Data format      | pandas, pyarrow      |
| Package manager  | poetry               |
| Docker           | Spark 3.0.1 on Ubuntu 18.04 |

## Project Layout

```
pyspark-pytest/
├── src/
│   ├── data_processing.py              ← Transaction classification pipeline
│   ├── reader/
│   │   └── spark_reader.py             ← CSV reader utility
│   ├── transformation/
│   │   └── df_transformation.py        ← DataFrame text transformations
│   └── utility/
│       ├── faker_customized_data.py    ← Custom Faker data generator
│       ├── faker_locale_data.py        ← Locale-aware Faker generator
│       ├── generate_csv_faker_data.py  ← CSV output Faker generator
│       └── generate_faker_data.py      ← JSON output Faker generator
├── tests/
│   ├── conftest.py                     ← Shared SparkSession fixture
│   ├── test_data_processing.py         ← Data processing pipeline tests
│   ├── dataframe/
│   │   ├── test_dataframe.py           ← DataFrame + SQL tests
│   │   └── test_df_equality.py         ← assertDataFrameEqual tests
│   ├── reader/
│   │   └── test_spark_reader.py        ← Mock-based reader tests
│   ├── spark_context/
│   │   └── test_spark_context.py       ← SparkContext tests
│   └── transformation/
│       └── test_df_transformation.py   ← Transformation tests
├── spark_docker/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── spark_docker.md
├── pyproject.toml
└── poetry.lock
```

## Conventions

### SparkSession

```python
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("descriptive-name")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

### Imports

```python
from pyspark.sql import functions as F
```

Never use `from pyspark.sql.functions import *`.

### Assertion Style

Use PySpark's built-in testing utilities:

```python
from pyspark.testing.utils import assertDataFrameEqual

assertDataFrameEqual(actual_df, expected_df)
```

### Package Manager

Use **poetry** for dependency management:

```bash
poetry install                  # install all deps
poetry add <package>            # add a runtime dependency
poetry add --group dev <pkg>    # add a dev dependency
```

### Running Tests

```bash
poetry run pytest               # run all tests
poetry run pytest tests/ -v     # verbose
```

### Docker

Build and run tests in Docker:

```bash
cd spark_docker
docker-compose build
docker-compose up
```

## Cleanup

Always call `spark.stop()` at the end of standalone scripts.
