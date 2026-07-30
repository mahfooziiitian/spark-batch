# Corrupt Record Column — Advanced Usage

Capture and recover data from type-mismatched and malformed records using
`_corrupt_record` / `columnNameOfCorruptRecord` in PERMISSIVE mode.

## Usage

```python title="examples/05_error_handling/02_rescued_data_column.py"
--8<-- "examples/05_error_handling/02_rescued_data_column.py"
```

!!! warning "rescueDataColumn is Databricks-Only"
    The `rescueDataColumn` option is **not available** in open-source Apache Spark.
    In OSS PySpark, use `_corrupt_record` with PERMISSIVE mode, then re-parse the
    corrupt lines with a relaxed all-string schema to recover data (shown in section 3).

!!! note
    On Databricks, `rescueDataColumn` captures individual mismatched field values
    (not the whole row), which is more granular than `_corrupt_record`. Useful for
    schema evolution when new fields appear in source JSON.

## Run

```bash
python examples/05_error_handling/02_rescued_data_column.py
```

## rescueDataColumn (Spark Connect / Databricks)

For Databricks or Spark Connect environments, `rescueDataColumn` provides
field-level rescue of type-mismatched and extra fields:

```python title="examples/05_error_handling/03_rescue_data_column_connect.py"
--8<-- "examples/05_error_handling/03_rescue_data_column_connect.py"
```

### Run (requires Spark Connect server)

```bash
# Start Spark Connect server first
SPARK_CONNECT_URL=sc://localhost:15002 python examples/05_error_handling/03_rescue_data_column_connect.py
```
