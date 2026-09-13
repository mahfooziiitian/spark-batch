# Extracting Links

`extract_links()` returns an `array<struct<text:string,href:string>>` column; `explode_links()`
extracts and explodes in a single call.

```python
--8<-- "examples/02_parsing/01_extract_links.py"
```
