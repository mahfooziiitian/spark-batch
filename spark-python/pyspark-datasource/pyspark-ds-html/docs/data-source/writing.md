# Writing HTML

`HtmlWriter` wraps `DataFrame.toPandas().to_html()`. Because rendering happens after a
`toPandas()` collect, this path is intended for small/aggregated result sets (reports,
dashboards) — not large distributed datasets.

```python
--8<-- "examples/03_dataframe/03_write_html.py"
```

## API surface

| Method | Description |
|--------|--------------|
| `render(df)` | Return the rendered `<table>` markup as a string |
| `write(df, path)` | Render and write a `<table>` fragment to a file |
| `write_page(df, path, title=...)` | Render and wrap in a minimal standalone HTML document |
| `.with_classes(css_classes)` | Apply CSS class(es) to the `<table>` element |
| `.with_index(enabled)` | Include/exclude the DataFrame index column |
| `.border(width)` | Set the table border width |
| `.table_id(id)` | Set the `id` attribute of the `<table>` element |

!!! warning "Collects to the driver"
    `render()`/`write()` call `toPandas()` internally. Aggregate or `.limit()` large
    DataFrames before writing to avoid driver out-of-memory errors.
