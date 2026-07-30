# Examples

Standalone PySpark 4 JSON examples organized by topic. Each script is independently
runnable and demonstrates a specific JSON feature.

## Prerequisites

```bash
# Generate test data fixtures
python generate_test_data.py

# Set Java 17 (required for PySpark 4)
export JAVA_HOME_17=/path/to/java-17
```

## Structure

```
examples/
├── 01_data_source/       # Reading JSON files (arrays, multiline, Pandas bridge)
├── 02_json_functions/    # Built-in functions: from_json, to_json, json_tuple, etc.
├── 03_dataframe/         # DataFrame operations on JSON data
├── 04_properties/        # All JSON read/write options (encoding, compression, etc.)
├── 05_error_handling/    # PERMISSIVE, DROPMALFORMED, FAILFAST, rescued data
├── 06_schema/            # Schema approaches: StructType, DDL, JSON, variable keys
└── 07_json_path/         # JSONPath expressions with get_json_object
```

## Running

```bash
# Run any example directly
python examples/01_data_source/read_json_array.py
python examples/04_properties/compression_gzip.py
python examples/05_error_handling/permissive_mode.py
```

All examples use the `pys_json` library for session management and configuration:

```python
from pys_json import get_spark, DATA_HOME, write_json_lines

spark = get_spark("my-example")
```
