# Extracting Meta Tags

`extract_meta_tags()` collects every `<meta name="..." content="...">` (or `property="..."`)
pair into a `map<string,string>` column, keyed by the tag's name/property.

```python
--8<-- "examples/02_parsing/03_extract_meta_tags.py"
```

!!! tip "Accessing map values"
    Spark map columns use bracket access, not dot notation: `col("meta")["author"]`, not
    `col("meta.author")`.
