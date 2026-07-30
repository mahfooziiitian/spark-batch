---
applyTo: "examples/**/*.py"
---

# Examples Instructions

## Structure

Examples are organized in numbered directories by topic:

```
examples/
├── 01_data_source/       # Reading JSON files
├── 02_json_functions/    # Built-in JSON functions
├── 04_properties/        # Read/write options
│   ├── compression/
│   ├── encoding/
│   ├── formatting/
│   ├── null_fields/
│   ├── parsing_rules/
│   └── type_coercion/
├── 05_error_handling/    # PERMISSIVE, DROPMALFORMED, FAILFAST
├── 06_schema/            # Schema definition approaches
└── 07_json_path/         # JSONPath expressions
```

## File Naming

- Use numbered prefixes within categories: `01_`, `02_`, etc.
- Names should be descriptive and concise (no `spark_` prefix)
- Use snake_case: `schema_inference.py`, `compression_gzip.py`

## Required Boilerplate

Every example file must:

1. Have a module-level docstring with key concepts and references
2. Import from `pys_json` library (not raw pyspark boilerplate)
3. Set `set_log_level("DEBUG")` at module level
4. Create a named logger
5. Use `if __name__ == "__main__":` guard
6. Call `spark.stop()` at the end

```python
"""Title — short description.

Key concepts:
    - Concept 1
    - Concept 2
"""

from pys_json import get_spark, print_header, print_schema, print_dataframe, set_log_level
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.name")


if __name__ == "__main__":
    spark = get_spark("example-name")

    # ... example code ...

    spark.stop()
```

## Output Conventions

- Use `print_header()` for section titles (not `print("===")`)
- Use `print_schema()` instead of `df.printSchema()`
- Use `print_dataframe()` instead of `df.show()`
- Use `print_path()` for file paths
- Use `print_success/warning/error()` for status messages
- Use `logger.info/debug()` for operational context

## Data Files

- Write test data inline using `write_json_lines()` from config
- Use `DATA_HOME` for all data paths
- Never hardcode absolute paths
- Generate data at the start of the example (self-contained)

## Imports

Use the `pys_json` library — never duplicate boilerplate:

```python
# Good
from pys_json import get_spark, DATA_HOME, write_json_lines

# Bad — raw boilerplate
from pyspark.sql import SparkSession
import os
os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
spark = SparkSession.builder.appName("x").master("local[*]").getOrCreate()
```

## Multi-Section Examples

For examples with multiple demonstrations, number the sections:

```python
print_header("1. Basic Usage")
# ...
print_header("2. Advanced Options")
# ...
print_header("3. Edge Cases")
# ...
```
