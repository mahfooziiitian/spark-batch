# Testing Guide

## SparkSession Fixture

Use a single **session-scoped** fixture to avoid JVM restart overhead across
the entire test run.

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .appName("test-pyspark-schema")
               .master("local[2]")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

## Test Class Layout

Organise tests into classes by schema area — one class per concept:

```python
class TestSchemaDefinition:    ...  # StructType, StructField, builder, fromDDL
class TestComplexTypes:        ...  # ArrayType, MapType, nested StructType
class TestSchemaIntrospection: ...  # printSchema, dtypes, simpleString, json
class TestColumnExistence:     ...  # has_column, AnalysisException behaviour
class TestSchemaValidation:    ...  # validate_schema, cast_to_schema
class TestSchemaComparison:    ...  # schema_diff, is_backward_compatible
class TestSchemaEvolution:     ...  # mergeSchema Parquet + compatibility
class TestSchemaFlattening:    ...  # flatten_schema, flatten_df
class TestDatesTimestamps:     ...  # DateType, TimestampType parsing
class TestSchemaMetadata:      ...  # metadata dict, PII tagging
```

## Schema Assertions

### Field presence and types

```python
def test_schema_fields(spark):
    schema = StructType.fromDDL("id BIGINT NOT NULL, name STRING")
    df = spark.createDataFrame([], schema)

    assert set(df.columns) == {"id", "name"}

    type_map = {f.name: type(f.dataType) for f in df.schema.fields}
    assert type_map["id"]   is LongType
    assert type_map["name"] is StringType
```

### Nullable flags

```python
def test_nullable_flags(spark):
    schema = StructType([
        StructField("id",   LongType(),   nullable=False),
        StructField("name", StringType(), nullable=True),
    ])
    df = spark.createDataFrame([], schema)

    nullable_map = {f.name: f.nullable for f in df.schema.fields}
    assert nullable_map["id"]   is False
    assert nullable_map["name"] is True
```

### Complex type structure

```python
def test_array_element_type(spark):
    schema = StructType.fromDDL("id BIGINT, tags ARRAY<STRING>")
    df = spark.createDataFrame([], schema)

    tags_field = df.schema["tags"]
    assert isinstance(tags_field.dataType, ArrayType)
    assert isinstance(tags_field.dataType.elementType, StringType)
```

### Row counts

Prefer `df.count()` over `len(df.collect())`:

```python
assert df.count() == 3
assert df.filter(F.col("id").isNull()).count() == 0
```

## File I/O Tests

Use `tmp_path` — unique per test and auto-cleaned:

```python
def test_schema_preserved_parquet(self, spark, tmp_path):
    schema = StructType([
        StructField("id",   LongType(),   nullable=False),
        StructField("name", StringType(), nullable=True),
    ])
    df = spark.createDataFrame([(1, "alice"), (2, "bob")], schema)
    path = str(tmp_path / "out.parquet")
    df.write.mode("overwrite").parquet(path)

    read_back = spark.read.parquet(path)
    assert set(read_back.columns) == {"id", "name"}
    assert read_back.count() == 2
```

## Running Tests

```bash
# Run all tests
pytest src/ -v

# Run a specific class
pytest src/ -v -k "TestSchemaValidation"

# Run with parallel workers
pytest src/ -v -n auto
```

## CI Environment Variables

```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export SPARK_LOCAL_IP=127.0.0.1
```

## Key Points

- One `SparkSession` per test run — share it via the session-scoped fixture.
- Use `spark.createDataFrame([], schema)` for structure-only tests that don't need rows.
- Use `tmp_path` for Parquet I/O tests — never hard-code `/tmp/` in tests.
- Set `spark.ui.enabled=false` and `shuffle.partitions=2` to speed up test startup.
