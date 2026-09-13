# Data Source

Spark has no built-in `spark.read.html()` — this project provides two bridges:

| Approach | Backend | Best for |
|----------|---------|----------|
| [Reading tables](reading-tables.md) | `pyspark.pandas.read_html()` (lxml/html5lib/bs4) | `<table>` elements — tabular data |
| [Scraping](scraping.md) | `BeautifulSoup` | Arbitrary tags — links, headings, meta tags, article bodies |
| [Writing](writing.md) | `pandas.to_html()` | Rendering DataFrame results as HTML reports |

All three are wrapped by the `pys_html.HtmlReader` / `pys_html.HtmlWriter` fluent APIs.
