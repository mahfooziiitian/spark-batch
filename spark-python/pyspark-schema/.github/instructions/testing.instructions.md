---
applyTo: "{**/test_*.py,**/*_test.py}"
---

# PySpark Schema Test Instructions

## SparkSession Fixture

Use a single session-scoped fixture to avoid JVM restart overhead:

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

## Test Organisation

Group tests by schema area:

```python
class TestSchemaDefinition:   ...  # StructType, StructField, builder, fromDDL
class TestComplexTypes:       ...  # ArrayType, MapType, nested StructType
class TestSchemaIntrospection:...  # printSchema, dtypes, simpleString, json
class TestColumnExistence:    ...  # has_column, AnalysisException behaviour
class TestSchemaValidation:   ...  # assert_schema, cast_to_schema
class TestSchemaEvolution:    ...  # mergeSchema Parquet + Delta Lake
class TestSchemaParser:       ...  # _parse_datatype_string round-trips
```

## Schema Assertions

### Field-level checks

```python
from pyspark.sql.types import StructType, LongType, StringType

def test_schema_has_expected_fields(spark):
    schema = StructType.fromDDL("id BIGINT NOT NULL, name STRING")
    df = spark.createDataFrame([], schema)

    field_names = [f.name for f in df.schema.fields]
    assert "id" in field_names
    assert "name" in field_names

def test_field_types(spark):
    schema = StructType.fromDDL("id BIGINT NOT NULL, name STRING")
    df = spark.createDataFrame([], schema)

    type_map = {f.name: f.dataType for f in df.schema.fields}
    assert isinstance(type_map["id"],   LongType)
    assert isinstance(type_map["name"], StringType)

def test_nullable_flags(spark):
    from pyspark.sql.types import StructField
    schema = StructType([
        StructField("id",   LongType(),   nullable=False),
        StructField("name", StringType(), nullable=True),
    ])
    df = spark.createDataFrame([], schema)

    nullable_map = {f.name: f.nullable for f in df.schema.fields}
    assert nullable_map["id"]   is False
    assert nullable_map["name"] is True
```

### Complex type checks

```python
from pyspark.sql.types import ArrayType, MapType, StructType

def test_array_element_type(spark):
    from pyspark.sql.types import StringType
    schema = StructType.fromDDL("id BIGINT, tags ARRAY<STRING>")
    df = spark.createDataFrame([], schema)

    tags_field = df.schema["tags"]
    assert isinstance(tags_field.dataType, ArrayType)
    assert isinstance(tags_field.dataType.elementType, StringType)

def test_nested_struct(spark):
    from pyspark.sql.types import StructField, StringType, LongType
    address = StructType([StructField("city", StringType(), nullable=True)])
    schema  = StructType([
        StructField("id",      LongType(),  nullable=False),
        StructField("address", address,     nullable=True),
    ])
    df = spark.createDataFrame([], schema)

    addr_field = df.schema["address"]
    assert isinstance(addr_field.dataType, StructType)
    assert "city" in [f.name for f in addr_field.dataType.fields]
```

### Row-count assertions

Prefer `df.count()` over `len(df.collect())`:

```python
assert df.count() == 3
assert df.filter(F.col("id").isNull()).count() == 0
```

## Column Existence Tests

```python
from pyspark.sql import Row

class TestColumnExistence:
    def test_top_level_column_exists(self, spark):
        df = spark.createDataFrame([Row(foo=1)])
        assert has_column(df, "foo") is True

    def test_missing_column_returns_false(self, spark):
        df = spark.createDataFrame([Row(foo=1)])
        assert has_column(df, "bar") is False

    def test_nested_column_path(self, spark):
        df = spark.sparkContext.parallelize(
            [Row(foo=Row(bar=Row(baz=3)))]
        ).toDF()
        assert has_column(df, "foo.bar.baz") is True
        assert has_column(df, "foo.bar.qux") is False
```

## Schema Round-Trip Tests

```python
import json
from pyspark.sql.types import StructType

def test_json_round_trip(spark):
    schema = StructType.fromDDL("id BIGINT NOT NULL, name STRING")
    schema_back = StructType.fromJson(json.loads(schema.json()))
    assert schema == schema_back

def test_ddl_round_trip(spark):
    schema = StructType.fromDDL("id BIGINT, name STRING")
    assert "id" in schema.simpleString()
    assert "name" in schema.simpleString()
```

## File I/O Tests

Use `tmp_path` — unique per test and cleaned up automatically:

```python
def test_schema_preserved_after_parquet_write(self, spark, tmp_path):
    from pyspark.sql.types import StructField, LongType, StringType
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

## Entry Point

Always include a direct-run entry point:

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```

## CI Environment Variables

```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export SPARK_LOCAL_IP=127.0.0.1
```
