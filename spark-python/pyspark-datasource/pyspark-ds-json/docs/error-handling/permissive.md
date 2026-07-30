# PERMISSIVE Mode

The default parse mode. Puts malformed records into a designated column and sets bad fields to null.

## Usage

```python title="examples/05_error_handling/01_error_modes.py"
--8<-- "examples/05_error_handling/01_error_modes.py"
```

!!! note
    The schema must include a `StringType` field matching the `columnNameOfCorruptRecord` option.
    If omitted, corrupt records are silently dropped.

!!! warning "Cache Before Querying `_corrupt_record`"
    Spark raises `UNSUPPORTED_FEATURE.QUERY_ONLY_CORRUPT_RECORD_COLUMN` when you filter
    or count on only the `_corrupt_record` column from a raw JSON read. Always `.cache()`
    the DataFrame before filtering:

    ```python
    df = spark.read.schema(schema).json(path).cache()
    corrupt = df.filter(df._corrupt_record.isNotNull())  # works after cache
    ```

## Run

```bash
python examples/05_error_handling/01_error_modes.py
```
