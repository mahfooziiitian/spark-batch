# Schema Evolution

Handling JSON schemas that change over time: new fields, type changes, removed fields, and versioning.

## Usage

```python title="examples/06_schema/11_schema_evolution.py"
--8<-- "examples/06_schema/11_schema_evolution.py"
```

## Evolution Strategies

| Strategy                          | When to Use                                    |
| --------------------------------- | ---------------------------------------------- |
| **Schema inference**              | Read multiple files, Spark unions the schemas  |
| **merge_schemas()**               | Programmatically combine known schema versions |
| **PERMISSIVE + \_corrupt_record** | Detect type changes in production              |
| **coalesce() defaults**           | Fill missing fields with default values        |
| **\_schema_version field**        | Route data through version-specific logic      |
| **Schema comparison**             | Detect drift across batches                    |

## Key Patterns

### New fields → null

```python
# Old data: {"id": 1, "name": "Alice"}
# New data: {"id": 2, "name": "Bob", "email": "bob@co.com"}
# Result: old rows get email=null automatically
df = spark.read.json([old_file, new_file])
```

### Type conflicts → widened to string

```python
# age was int in v1, string in v2 → Spark widens to StringType
# Use PERMISSIVE + strict schema to detect this
```

### Schema versioning

```python
# Add _schema_version to your records
# Then normalize: coalesce() missing fields, cast changed types
```

!!! warning "Cache Required"
When using `_corrupt_record` to detect type changes, always `.cache()` the
DataFrame before filtering on the corrupt record column.

## Run

```bash
python examples/06_schema/11_schema_evolution.py
```
