# Cookbook

Quick recipes for common PyDeequ tasks. Each recipe is self-contained
and can be copy-pasted into your pipeline.

## Basic Verification

Check that a DataFrame meets minimum quality standards:

```python
import os
os.environ["SPARK_VERSION"] = "3.5"

import pydeequ
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
    .config("spark.jars.packages", pydeequ.deequ_maven_coord)
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    .master("local[*]").getOrCreate())

df = spark.createDataFrame([
    ("alice", 30, "alice@example.com"),
    ("bob", 25, "bob@example.com"),
    ("charlie", 35, None),
], ["name", "age", "email"])

check = (
    Check(spark, CheckLevel.Error, "basic-check")
    .hasSize(lambda x: x >= 1)
    .isComplete("name")
    .isNonNegative("age")
)

result = VerificationSuite(spark).onData(df).addCheck(check).run()
result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
result_df.show(truncate=False)

spark.stop()
```

## Compute Multiple Metrics

Run several analyzers in one pass:

```python
from pydeequ.analyzers import (
    AnalysisRunner, AnalyzerContext,
    Size, Completeness, Mean, ApproxCountDistinct,
)

result = (
    AnalysisRunner(spark)
    .onData(df)
    .addAnalyzer(Size())
    .addAnalyzer(Completeness("email"))
    .addAnalyzer(Mean("age"))
    .addAnalyzer(ApproxCountDistinct("name"))
    .run()
)

metrics_df = AnalyzerContext.successMetricsAsDataFrame(spark, result)
metrics_df.show(truncate=False)
```

??? example "Expected Output"
    | entity | instance | name | value |
    | --- | --- | --- | --- |
    | Dataset | * | Size | 3.0 |
    | Column | email | Completeness | 0.667 |
    | Column | age | Mean | 30.0 |
    | Column | name | ApproxCountDistinct | 3.0 |

## Profile All Columns

Get a statistical summary of every column:

```python
from pydeequ.profiles import ColumnProfilerRunner

result = ColumnProfilerRunner(spark).onData(df).run()

for col_name, profile in result.profiles.items():
    print(f"\n--- {col_name} ---")
    print(f"  Type: {profile.dataType}")
    print(f"  Completeness: {profile.completeness:.2%}")
    print(f"  Approx Distinct: {profile.approximateNumDistinctValues}")
```

## Auto-Suggest Constraints

Let PyDeequ analyze your data and propose rules:

```python
from pydeequ.suggestions import ConstraintSuggestionRunner, DEFAULT

suggestions = (
    ConstraintSuggestionRunner(spark)
    .onData(df)
    .addConstraintRule(DEFAULT())
    .run()
)

# Print each suggested constraint
for suggestion in suggestions.get("constraint_suggestions", []):
    print(f"  {suggestion['constraint_name']}: {suggestion['description']}")
```

## Persist Metrics for Trend Analysis

Save metrics with a timestamp for historical comparison:

```python
from pydeequ.repository import FileSystemMetricsRepository, ResultKey
from pydeequ.analyzers import AnalysisRunner, Size, Completeness

# Setup repository
metrics_path = FileSystemMetricsRepository.helper_metrics_file(spark, "/tmp/metrics.json")
repository = FileSystemMetricsRepository(spark, metrics_path)

# Tag this run
tags = {"pipeline": "user_etl", "env": "production"}
result_key = ResultKey(spark, ResultKey.current_milli_time(), tags)

# Compute and save
AnalysisRunner(spark) \
    .onData(df) \
    .addAnalyzer(Size()) \
    .addAnalyzer(Completeness("email")) \
    .useRepository(repository) \
    .saveOrAppendResult(result_key) \
    .run()

# Query saved metrics
historical = repository.load() \
    .before(ResultKey.current_milli_time()) \
    .getSuccessMetricsAsDataFrame()
historical.show()
```

## Check for Schema Changes

Combine PySpark schema inspection with PyDeequ:

```python
# Verify expected columns exist
expected_columns = {"name", "age", "email", "created_at"}
actual_columns = set(df.columns)

missing = expected_columns - actual_columns
if missing:
    raise ValueError(f"Missing columns: {missing}")

# Then run type-aware constraints
check = (
    Check(spark, CheckLevel.Error, "schema-check")
    .isComplete("name")
    .hasDataType("age", ConstrainableDataTypes.Integral)
)
```

## Conditional Constraints

Apply different checks based on data characteristics:

```python
row_count = df.count()

check = Check(spark, CheckLevel.Error, "adaptive-check")
check = check.isComplete("id").isUnique("id")

# Only enforce completeness threshold on large datasets
if row_count > 1000:
    check = check.hasCompleteness("email", lambda x: x >= 0.95)
else:
    check = check.hasCompleteness("email", lambda x: x >= 0.8)

result = VerificationSuite(spark).onData(df).addCheck(check).run()
```
