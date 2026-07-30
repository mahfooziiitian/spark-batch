# Pandas Bridge

Converting between Pandas and Spark DataFrames, and using Pandas UDFs.

## Usage

```python title="examples/03_dataframe/05_pandas_bridge.py"
--8<-- "examples/03_dataframe/05_pandas_bridge.py"
```

!!! tip "When to Use Pandas Bridge"
    - Reading exotic JSON formats that Spark doesn't natively support
    - Applying complex transformations using Pandas/NumPy logic
    - Small-to-medium datasets where Pandas parsing is simpler
    - Leveraging Pandas UDFs for vectorized operations in Spark

## Run

```bash
python examples/03_dataframe/05_pandas_bridge.py
```
