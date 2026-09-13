# Parsing

Column-level UDFs for extracting structure from an existing DataFrame column of raw HTML
strings (e.g., pages you've already scraped/stored), without leaving the DataFrame API.

| Function | Returns | Description |
|----------|---------|--------------|
| [`strip_html_tags(col)`](extract-text.md) | `string` | Visible text with tags removed |
| [`extract_tag_text(col, tag)`](extract-text.md) | `array<string>` | Text of every occurrence of a tag |
| [`extract_links(col)`](extract-links.md) | `array<struct<text,href>>` | Every `<a>` link |
| [`extract_meta_tags(col)`](extract-meta.md) | `map<string,string>` | Every `<meta name=/content=>` pair |
| `explode_links(df, html_col, link_col)` | DataFrame | Convenience: extract + explode links in one call |

All functions are implemented as `pyspark.sql.functions.udf()` wrappers around BeautifulSoup
and are importable from `pys_html.parsing`.
