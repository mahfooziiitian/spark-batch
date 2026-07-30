# Pandas Bridge

Convert between Pandas DataFrames and Spark DataFrames when working with JSON data.

## Usage

```python title="examples/01_data_source/read_json_pandas.py"
--8<-- "examples/01_data_source/read_json_pandas.py"
```

!!! tip
    The Pandas bridge is useful when you need to read JSON formats that Spark doesn't
    natively support, or when you need Pandas-specific parsing features before scaling
    out with Spark.

## Run

```bash
python examples/01_data_source/read_json_pandas.py
```
