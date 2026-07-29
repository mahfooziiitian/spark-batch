# Schema Utilities

Schema inspection and comparison utilities for working with PySpark's
`StructType` schemas.

## Source

```python title="src/data_frame/schema/schema_utils.py"
--8 < --"src/data_frame/schema/schema_utils.py"
```

## Functions

### get_column_names_by_type

Returns column names matching a given Spark data type. Useful for selecting
all string or numeric columns dynamically.

```python
from data_frame.schema.schema_utils import get_column_names_by_type

string_cols = get_column_names_by_type(df, "string")  # ["name", "city"]
long_cols = get_column_names_by_type(df, "long")  # ["id", "count"]
```

### schema_to_dict

Converts a `StructType` schema to a `{name: type}` dictionary for easy
inspection and comparison.

```python
from data_frame.schema.schema_utils import schema_to_dict

schema_to_dict(df.schema)
# {"id": "long", "name": "string", "score": "double"}
```

### add_nullable_fields

Returns a copy of the schema with all fields set to nullable. Useful when
comparing schemas from different sources where nullable flags may differ.

```python
from data_frame.schema.schema_utils import add_nullable_fields
from chispa.schema_comparer import assert_schema_equality

relaxed = add_nullable_fields(strict_schema)
assert_schema_equality(relaxed, other_schema)
```

## Run Tests

```bash
uv run pytest tests/schema/ -v
```
