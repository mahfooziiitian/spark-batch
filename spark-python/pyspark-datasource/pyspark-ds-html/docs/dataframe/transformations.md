# Transformations

HTML tables carry no type information, so every extracted column arrives as `StringType`.
Cast and derive columns explicitly once the raw table is loaded:

```python
--8<-- "examples/03_dataframe/02_transformations.py"
```
