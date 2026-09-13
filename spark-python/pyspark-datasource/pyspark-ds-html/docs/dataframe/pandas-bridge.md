# Pandas Bridge

A full round trip — Spark DataFrame → pandas → HTML → pandas (`read_html`) → Spark DataFrame —
useful for validating writer output or when downstream systems only accept small HTML files.

```python
--8<-- "examples/03_dataframe/04_pandas_bridge.py"
```
