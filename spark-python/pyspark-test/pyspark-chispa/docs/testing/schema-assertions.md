# Schema Assertions

Schema assertions compare `StructType` schemas — useful for validating
that transformations preserve or modify schemas correctly.

## assert_schema_equality

```python
from chispa.schema_comparer import assert_schema_equality
```

!!! warning "Pass schemas, not DataFrames"
    `assert_schema_equality` expects `StructType` objects. Always use
    `df.schema`, not `df` itself.

### Matching Schemas

```python
def test_matching_schemas(self, spark):
    df1 = spark.createDataFrame([(1, 4)], ["num", "val"])
    df2 = spark.createDataFrame([(5, 6)], ["num", "val"])
    assert_schema_equality(df1.schema, df2.schema)
```

### Mismatched Schemas

When schemas differ, chispa produces a clear side-by-side diff:

```
+-------------------------------------------+---------------------------------------+
|                  schema1                  |                schema2                |
+-------------------------------------------+---------------------------------------+
|    StructField('num', LongType(), True)   |  StructField('num', LongType(), True) |
| StructField('letter', StringType(), True) | StructField('num2', LongType(), True) |
+-------------------------------------------+---------------------------------------+
```

Test that mismatches raise:

```python
def test_same_columns_different_types_raises(self, spark):
    schema1 = StructType([
        StructField("id", LongType(), True),
        StructField("val", StringType(), True),
    ])
    schema2 = StructType([
        StructField("id", LongType(), True),
        StructField("val", DoubleType(), True),
    ])
    with pytest.raises(Exception):
        assert_schema_equality(schema1, schema2)
```

### Constructing Schemas Programmatically

You don't need a DataFrame to compare schemas. Build them directly:

```python
from pyspark.sql.types import StructType, StructField, LongType, StringType

schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
])
```

### Nested Schemas

```python
def test_nested_struct_schemas(self, spark):
    nested = StructType([
        StructField("id", LongType(), True),
        StructField("info", StructType([
            StructField("name", StringType(), True),
        ]), True),
    ])
    assert_schema_equality(nested, nested)
```

### Empty Schemas

```python
def test_empty_schemas_match(self, spark):
    assert_schema_equality(StructType([]), StructType([])
```

## Schema Utilities

This project provides helper functions for working with schemas:

### get_column_names_by_type

Filter columns by their Spark data type:

```python
from data_frame.schema.schema_utils import get_column_names_by_type

string_cols = get_column_names_by_type(df, "string")  # ["name", "city"]
```

### schema_to_dict

Convert a schema to a dictionary for inspection:

```python
from data_frame.schema.schema_utils import schema_to_dict

schema_to_dict(df.schema)
# {"id": "long", "name": "string", "score": "double"}
```

### add_nullable_fields

Make all fields nullable — useful when comparing schemas from sources with
different nullable conventions:

```python
from data_frame.schema.schema_utils import add_nullable_fields

relaxed = add_nullable_fields(strict_schema)
assert_schema_equality(relaxed, other_schema)
```

## Run Tests

```bash
uv run pytest tests/schema/ -v
```
