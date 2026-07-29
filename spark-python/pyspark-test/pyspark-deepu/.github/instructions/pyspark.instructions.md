---
applyTo: "spark-python/pyspark-test/pyspark-deepu/src/**/*.py"
---

# PyDeequ Source Code Instructions

## SPARK_VERSION Environment Variable

PyDeequ reads `SPARK_VERSION` at import time. Always set it at module level
**before** any pydeequ import:

```python
import os
os.environ["SPARK_VERSION"] = "3.5"

import pydeequ  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
```

## Imports

Use explicit imports — never `from pydeequ.analyzers import *`:

```python
from pydeequ.analyzers import AnalysisRunner, Size, Completeness, Mean
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from pyspark.sql import functions as F
```

## SparkSession

Always configure the PyDeequ JAR and use SPARK_MASTER env var:

```python
spark = (SparkSession.builder
         .appName("descriptive-name")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.jars.packages", pydeequ.deequ_maven_coord)
         .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

## Script Structure

Each source file is a runnable demo with extractable functions:

```python
"""Module docstring describing the PyDeequ feature demonstrated."""

import os
os.environ["SPARK_VERSION"] = "3.5"

import pydeequ  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402


def run_demo(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Run the PyDeequ feature and return results.

    Args:
        spark: Active SparkSession.
        df: Input DataFrame.

    Returns:
        DataFrame containing results.
    """
    ...


def main() -> None:
    """Run the PyDeequ demo."""
    spark = (SparkSession.builder
             .appName("demo-name")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.jars.packages", pydeequ.deequ_maven_coord)
             .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # ... demo code ...

    spark.stop()

if __name__ == "__main__":
    main()
```

## Type Hints

Add type hints to function signatures:

```python
from pyspark.sql import DataFrame

def run_analysis(spark: SparkSession, df: DataFrame) -> DataFrame:
    ...
```

## Docstrings

Use Google-style docstrings on functions:

```python
def run_verification(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Run constraint verification on the given DataFrame.

    Args:
        spark: Active SparkSession.
        df: Input DataFrame to verify.

    Returns:
        DataFrame containing verification results.
    """
```

## Cleanup

Always call `spark.stop()` at the end of standalone scripts.
