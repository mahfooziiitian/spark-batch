# JSONPath Expressions

JSONPath expressions are used to extract values from JSON strings using the `get_json_object`
function or the `:` operator (Spark 3.5+).

## Overview

| Syntax | Description | Example |
|--------|-------------|---------|
| `$` | Root object | `$` |
| `.key` | Child operator | `$.name` |
| `['key']` | Bracket notation | `$['first name']` |
| `[n]` | Array index | `$.items[0]` |
| `[*]` | Array wildcard | `$.items[*].name` |

## Usage

```python title="examples/07_json_path/01_json_path_expressions.py" hl_lines="5-8"
--8<-- "examples/07_json_path/01_json_path_expressions.py:40:55"
```

## Common Patterns

### Extract Nested Field

```python
from pyspark.sql.functions import get_json_object, col

df.withColumn("city", get_json_object(col("json_col"), "$.address.city"))
```

### Extract Array Element

```python
df.withColumn("first_item", get_json_object(col("json_col"), "$.items[0].name"))
```

### Extract All Array Elements

```python
df.withColumn("all_names", get_json_object(col("json_col"), "$.items[*].name"))
```

## JSONPath vs from_json

| Approach | Best For |
|----------|----------|
| `get_json_object` + JSONPath | Quick extraction of 1-2 deeply nested fields |
| `from_json` | Full parsing of complex structures into typed columns |
| `json_tuple` | Extracting multiple top-level keys efficiently |

!!! tip
    Use JSONPath when you need a specific nested value without parsing the entire structure.
    For complex multi-field access, prefer `from_json` with a proper schema.

## Run

```bash
python examples/07_json_path/json_path_expression.py
```
