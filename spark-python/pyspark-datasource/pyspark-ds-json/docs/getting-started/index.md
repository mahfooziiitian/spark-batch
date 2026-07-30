# Getting Started

Set up your environment and run your first PySpark JSON example in under 5 minutes.

## Prerequisites

| Component | Version | Notes |
|-----------|---------|-------|
| Python | ≥ 3.11 | 3.12 also supported |
| Java | 17+ | Required by PySpark 4.x |
| PySpark | 4.x | Installed via pip/uv |

!!! warning "Java 17 Required"
    PySpark 4.x requires Java 17 or later. Set `JAVA_HOME` to your Java 17 installation.

## Installation

=== "uv (Recommended)"
    ```bash
    # Clone the repository
    git clone https://github.com/mahfooziiitian/spark-batch.git
    cd spark-batch/spark-python/pyspark-datasource/pyspark-ds-json

    # Create virtual environment and install
    uv sync
    ```

=== "pip"
    ```bash
    # Clone the repository
    git clone https://github.com/mahfooziiitian/spark-batch.git
    cd spark-batch/spark-python/pyspark-datasource/pyspark-ds-json

    # Create virtual environment
    python -m venv .venv
    source .venv/bin/activate

    # Install dependencies
    pip install -e ".[dev]"
    ```

## Project Structure

```
pyspark-ds-json/
├── src/pys_json/          # Reusable library
│   ├── reader/            # JSON reader utilities
│   ├── writer/            # JSON writer utilities
│   ├── parsing/           # from_json, to_json, json_tuple wrappers
│   ├── schema/            # Schema builders, validators, converters
│   └── config.py          # Environment configuration
├── examples/              # 60+ standalone examples (7 categories)
│   ├── 01_data_source/    # spark.read.json / spark.write.json
│   ├── 02_json_functions/ # Built-in JSON functions
│   ├── 03_dataframe/      # DataFrame patterns
│   ├── 04_properties/     # Read/write options
│   ├── 05_error_handling/ # PERMISSIVE, DROPMALFORMED, FAILFAST
│   ├── 06_schema/         # Schema approaches and validation
│   └── 07_json_path/      # JSONPath expressions
├── docs/                  # MkDocs Material documentation
├── tests/                 # pytest test suite
└── Makefile               # Common dev commands
```

## Run Your First Example

```bash
# Read a JSON array into a DataFrame
python examples/01_data_source/read_json_array.py
```

Expected output (Rich formatted):

```
╭──────────────────────────────────────────────────────────╮
│              1. Read JSON Array File                      │
╰──────────────────────────────────────────────────────────╯
┏━━━━━┳━━━━━━━━┳━━━━━┓
┃ id  ┃ name   ┃ age ┃
┡━━━━━╇━━━━━━━━╇━━━━━┩
│ 1   │ Alice  │ 30  │
│ 2   │ Bob    │ 25  │
└─────┴────────┴─────┘
✓ JSON array loaded successfully
```

## Using the Library

The `pys_json` package provides utilities for common JSON operations:

```python
from pys_json import get_spark, print_dataframe, print_header
from pys_json.schema import schema_from_dict, validate_schema_file
from pys_json.parsing import parse_json_column

# Create SparkSession (configures Java, log level automatically)
spark = get_spark("my-app")

# Read JSON with schema
df = spark.read.json("data/events.json")
print_dataframe(df, title="Events")

spark.stop()
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARK_MASTER` | `local[*]` | Spark master URL |
| `JAVA_HOME_17` | — | Path to Java 17 installation |
| `PYS_JSON_LOG_LEVEL` | `WARNING` | Library log level (DEBUG, INFO, WARNING) |
| `PYS_JSON_DATA_HOME` | `/tmp/pys_json` | Default data directory |

## Make Commands

```bash
make help          # Show all available commands
make format        # Format code with ruff
make lint          # Run ruff linter
make type-check    # Run mypy type checking
make test          # Run pytest
make check-all     # Run all checks (format, lint, mypy, bandit)
make docs          # Build documentation
make docs-serve    # Serve docs locally at localhost:8000
```

## Next Steps

<div class="grid cards" markdown>

-   :material-file-document:{ .lg .middle } **[Data Source](../data-source/index.md)**

    ---

    Learn to read and write JSON files

-   :material-function:{ .lg .middle } **[JSON Functions](../json-functions/index.md)**

    ---

    Parse JSON strings with from_json, to_json, json_tuple

-   :material-code-braces:{ .lg .middle } **[Schema](../schema/index.md)**

    ---

    Define, infer, validate, and evolve schemas

-   :material-alert-circle:{ .lg .middle } **[Error Handling](../error-handling/index.md)**

    ---

    Handle malformed JSON with PERMISSIVE, DROPMALFORMED, FAILFAST

</div>
