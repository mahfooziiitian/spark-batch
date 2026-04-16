---
applyTo: "spark-python/pyspark-test/pyspark-deepu/src/**/*.py"
---

# PyDeequ Source Code Instructions

## Imports

```python
import os
import pydeequ
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
```

Avoid `from pydeequ.analyzers import *` — use explicit imports:

```python
from pydeequ.analyzers import AnalysisRunner, Size, Completeness, Mean
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
```

## SparkSession

Always configure the PyDeequ JAR and set the Spark version:

```python
os.environ["SPARK_VERSION"] = "3.0.2"

spark = (SparkSession.builder
         .appName("descriptive-name")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.jars.packages", pydeequ.deequ_maven_coord)
         .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

## Script Structure

Each source file is a runnable demo script:

```python
"""Module docstring describing the PyDeequ feature demonstrated."""

import os
import pydeequ
from pyspark.sql import SparkSession

def main():
    """Run the PyDeequ demo."""
    os.environ["SPARK_VERSION"] = "3.0.2"

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

Add type hints to function signatures where practical:

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
