---
applyTo: "tests/**/*.py"
---

# PySpark DataFrame — Test Instructions

## Shared SparkSession Fixture (conftest.py)

Define **one** session-scoped fixture in `tests/conftest.py` and reuse it across all
test modules. Never create a new `SparkSession` inside individual test files.

```python
# tests/conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .appName("pyspark-dataframe-tests")
               .master("local[2]")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled",             "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

**Anti-patterns to avoid:**

```python
# ❌ per-file fixture — creates a second JVM session
@pytest.fixture(scope="session")
def spark_session():   # wrong name AND wrong scope pattern
    ...

# ❌ per-test session (JVM restart per test = very slow)
@pytest.fixture(scope="function")
def spark():
    ...

# ❌ hardcoded env vars inside test files
os.environ["JAVA_HOME"] = "E:\\..."
```

Always name the fixture `spark` (not `spark_session`) so every test file injects it
with `def test_foo(self, spark)`.

## Test Configuration (pyproject.toml)

The pytest settings in `pyproject.toml` set the `src/` directory on `PYTHONPATH`
so tests can import source modules directly:

```toml
[tool.pytest.ini_options]
minversion = "6.0"
addopts   = "-ra -q"
pythonpath = ["src"]
testpaths  = ["tests"]
```

## Test Class Organisation

Group tests into classes that mirror the `src/` module structure:

```python
class TestDataFrameCreation:   ...  # from tuples, dicts, JSON, explicit schema
class TestColumnOperations:    ...  # withColumn, withColumns, select, drop, rename, when/otherwise
class TestJoins:               ...  # inner, left, right, full, semi, anti, cross, broadcast, natural
class TestWindowFunctions:     ...  # rank, dense_rank, row_number, lag, lead, running total
class TestAggregations:        ...  # groupBy, pivot, countDistinct, rollup, cube
class TestTransformations:     ...  # filter, sort, dedup, union, sampling, partition
class TestSchemaValidation:    ...  # printSchema, dtypes, StructType matching
class TestNullHandling:        ...  # dropna, fillna, coalesce, isNull, isNotNull
class TestDateTimeFunctions:   ...  # to_date, to_timestamp, date_add, datediff
class TestParquetIO:           ...  # write/read parquet, partitioned output
class TestETL:                 ...  # end-to-end pipeline correctness
```

## Naming Conventions

- Test methods: `test_<what>_<condition>` — e.g., `test_inner_join_returns_only_matching_rows`.
- Test classes: `Test<Topic>` — matches the capability area, not the source file name.
- Test files: `test_<topic>.py` — one file per test class is fine; group related classes if small.

## Assertions

### Row count

```python
assert df.count() == 5
```

Prefer `df.count()` over `len(df.collect())` — never pull all rows to the driver unnecessarily.

### Schema (column names and types)

```python
assert set(df.columns) == {"id", "name", "region", "revenue"}

from pyspark.sql.types import DoubleType
assert df.schema["revenue"].dataType == DoubleType()
```

### Filtered count

```python
from pyspark.sql import functions as F

assert df.filter(F.col("region") == "North").count() == 3
```

### Single-row value assertion

```python
row = df.filter(F.col("region") == "North").first()
assert row["total_revenue"] == pytest.approx(1999.98, rel=1e-4)
```

### Ordered result assertion (collect only what you need)

```python
rows = (df
        .orderBy("id")
        .select("id", "rank")
        .collect())
assert [(r["id"], r["rank"]) for r in rows] == [(1, 1), (2, 2), (3, 3)]
```

### No-duplicate assertion

```python
assert df.count() == df.dropDuplicates(["id"]).count()
```

### Null value assertions

```python
# Assert a column has no nulls
assert df.filter(F.col("id").isNull()).count() == 0

# Assert specific rows have null in a column
assert df.filter(F.col("discount").isNull()).count() == 2

# Assert coalesce result is never null
filled = df.withColumn("val", F.coalesce(F.col("override"), F.lit(0)))
assert filled.filter(F.col("val").isNull()).count() == 0
```

### Approximate float equality

```python
row = df.filter(F.col("region") == "North").first()
assert row["total_revenue"] == pytest.approx(1999.98, rel=1e-4)
```

## Join Tests

Always create minimal, self-contained datasets for both sides of the join:

```python
def test_inner_join(self, spark):
    employees = spark.createDataFrame(
        [(1, "Alice", 10), (2, "Bob", 99)],
        ["emp_id", "name", "dept_id"],
    )
    departments = spark.createDataFrame(
        [(10, "Engineering")],
        ["dept_id", "dept_name"],
    )
    result = employees.join(departments, on=["dept_id"], how="inner")
    assert result.count() == 1
    assert result.first()["name"] == "Alice"
```

Test every meaningful join variant in its own method:
- `test_inner_join` — only matching rows
- `test_left_outer_join` — all left rows, nulls on right for non-matches
- `test_right_outer_join` — all right rows, nulls on left
- `test_full_outer_join` — all rows from both sides
- `test_left_semi_join` — left rows where a match exists (no right columns)
- `test_left_anti_join` — left rows where no match exists
- `test_cross_join` — cartesian product (assert count == left × right)
- `test_broadcast_join` — same result as inner join, verify no shuffle via explain

## Window Function Tests

Always use a deterministic, ordered dataset and assert exact values:

```python
def test_running_total(self, spark):
    from pyspark.sql.window import Window

    data = [(1, "A", 100), (2, "A", 200), (3, "A", 300)]
    df = spark.createDataFrame(data, ["id", "region", "revenue"])

    w = (Window
         .partitionBy("region")
         .orderBy("id")
         .rowsBetween(Window.unboundedPreceding, Window.currentRow))

    result = df.withColumn("running_total", F.sum("revenue").over(w))
    rows = result.orderBy("id").select("id", "running_total").collect()

    assert [(r["id"], r["running_total"]) for r in rows] == [
        (1, 100), (2, 300), (3, 600)
    ]

def test_rank_within_partition(self, spark):
    from pyspark.sql.window import Window

    data = [("North", 500), ("North", 200), ("North", 800), ("South", 300)]
    df = spark.createDataFrame(data, ["region", "revenue"])

    w = Window.partitionBy("region").orderBy(F.desc("revenue"))
    result = df.withColumn("rank", F.rank().over(w))

    north = result.filter(F.col("region") == "North").orderBy("rank").collect()
    assert [r["rank"] for r in north] == [1, 2, 3]
```

## Aggregation Tests

```python
def test_group_by_sum(self, spark):
    data = [("North", 100.0), ("North", 200.0), ("South", 150.0)]
    df = spark.createDataFrame(data, ["region", "revenue"])

    result = df.groupBy("region").agg(F.sum("revenue").alias("total"))
    north_row = result.filter(F.col("region") == "North").first()

    assert north_row["total"] == pytest.approx(300.0)

def test_pivot(self, spark):
    data = [("Alice", "Q1", 100), ("Alice", "Q2", 200), ("Bob", "Q1", 50)]
    df = spark.createDataFrame(data, ["name", "quarter", "revenue"])

    result = df.groupBy("name").pivot("quarter", ["Q1", "Q2"]).sum("revenue")
    alice = result.filter(F.col("name") == "Alice").first()

    assert alice["Q1"] == 100
    assert alice["Q2"] == 200
```

## Schema Tests

```python
def test_explicit_schema(self, spark):
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType

    schema = StructType([
        StructField("id",   IntegerType(), nullable=False),
        StructField("name", StringType(),  nullable=True),
    ])
    df = spark.createDataFrame([(1, "Alice")], schema)

    assert df.schema["id"].dataType   == IntegerType()
    assert df.schema["name"].dataType == StringType()
    assert df.schema["id"].nullable   is False
```

## File I/O Tests

Use `tmp_path` — never hardcode output paths:

```python
def test_parquet_round_trip(self, spark, tmp_path):
    path = str(tmp_path / "output.parquet")
    data = [(1, "Alice", 100.0), (2, "Bob", 200.0)]
    df = spark.createDataFrame(data, ["id", "name", "revenue"])

    df.write.mode("overwrite").parquet(path)
    read_back = spark.read.parquet(path)

    assert read_back.count() == 2
    assert set(read_back.columns) == {"id", "name", "revenue"}

def test_partitioned_parquet(self, spark, tmp_path):
    path = str(tmp_path / "partitioned.parquet")
    data = [("North", 2024, 100.0), ("South", 2024, 200.0)]
    df = spark.createDataFrame(data, ["region", "year", "revenue"])

    df.write.mode("overwrite").partitionBy("region").parquet(path)
    read_back = spark.read.parquet(path)

    assert read_back.count() == 2
```

## Transformation Tests

```python
class TestTransformations:

    def test_filter(self, spark):
        df = spark.createDataFrame([(1, "active"), (2, "inactive")], ["id", "status"])
        assert df.filter(F.col("status") == "active").count() == 1

    def test_deduplication(self, spark):
        df = spark.createDataFrame([(1, "a"), (1, "a"), (2, "b")], ["id", "val"])
        assert df.dropDuplicates(["id"]).count() == 2

    def test_union(self, spark):
        df1 = spark.createDataFrame([(1, "a")], ["id", "val"])
        df2 = spark.createDataFrame([(2, "b")], ["id", "val"])
        assert df1.unionByName(df2).count() == 2

    def test_sort_descending(self, spark):
        df = spark.createDataFrame([(3,), (1,), (2,)], ["val"])
        top = df.orderBy(F.desc("val")).first()["val"]
        assert top == 3
```

## DataFrame Equality with chispa

Use [`chispa`](https://github.com/MrPowers/chispa) (already in `dev.dependencies`) for
whole-DataFrame equality assertions — it shows clear diffs on mismatch:

```python
from chispa import assert_df_equality

def test_revenue_rounding(self, spark):
    source = spark.createDataFrame([(1, 1.005), (2, 2.999)], ["id", "rev"])
    expected = spark.createDataFrame([(1, 1.01), (2, 3.00)], ["id", "rev"])
    result = source.withColumn("rev", F.round(F.col("rev"), 2))
    assert_df_equality(result, expected)

# Ignore row order when result order is non-deterministic
def test_aggregation_result(self, spark):
    ...
    assert_df_equality(result, expected, ignore_row_order=True)

# Ignore nullable flag differences in schema
def test_schema_nullable_mismatch(self, spark):
    ...
    assert_df_equality(result, expected, ignore_nullable=True)
```

Prefer `assert_df_equality` over manual `collect()` comparisons when asserting full
DataFrame correctness. Use `df.count()` + `.first()` only for lightweight spot-checks.

## Data-Driven Tests with pytest.mark.parametrize

```python
import pytest
from pyspark.sql import functions as F

@pytest.mark.parametrize("revenue,expected_tier", [
    (1500.0, "Gold"),
    (750.0,  "Silver"),
    (100.0,  "Bronze"),
])
def test_revenue_tier(self, spark, revenue, expected_tier):
    df = spark.createDataFrame([(1, revenue)], ["id", "revenue"])
    result = df.withColumn(
        "tier",
        F.when(F.col("revenue") >= 1000, "Gold")
         .when(F.col("revenue") >= 500,  "Silver")
         .otherwise("Bronze"),
    )
    assert result.first()["tier"] == expected_tier
```

Use `parametrize` whenever the same logic should be verified for multiple input values
or boundary conditions.

## Date and Timestamp Tests

```python
def test_date_parsing(self, spark):
    df = spark.createDataFrame([("2024-01-15",)], ["date_str"])
    result = df.withColumn("event_date", F.to_date(F.col("date_str"), "yyyy-MM-dd"))

    assert result.schema["event_date"].dataType == DateType()
    assert result.filter(F.col("event_date").isNull()).count() == 0

def test_datediff(self, spark):
    df = spark.createDataFrame([("2024-01-01", "2024-01-31")], ["start", "end"])
    result = df.withColumn(
        "days",
        F.datediff(F.to_date("end", "yyyy-MM-dd"), F.to_date("start", "yyyy-MM-dd")),
    )
    assert result.first()["days"] == 30
```

## Entry Point

```python
if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v", "--tb=short"])
```
