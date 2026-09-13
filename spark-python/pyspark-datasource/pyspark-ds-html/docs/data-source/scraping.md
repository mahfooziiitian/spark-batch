# Scraping with BeautifulSoup

For content that isn't laid out as a `<table>` — links, headings, article bodies — use
`HtmlReader.read_elements()` (and its convenience wrappers `read_links()` / `read_text()`),
which parse the document with BeautifulSoup and materialize matches as a Spark DataFrame.

```python
--8<-- "examples/01_data_source/03_scrape_with_beautifulsoup.py"
```

## API surface

| Method | Returns | Description |
|--------|---------|--------------|
| `read_elements(source, tag, attrs=None)` | DataFrame | Every element matching `tag` (+ optional attribute filter), with `text` and captured attribute columns |
| `read_links(source)` | DataFrame | Shortcut for `read_elements(source, "a")` — `text` + `href` columns |
| `read_text(source)` | `str` | All visible text in the document, tags stripped |

## Choosing a parser backend

`HtmlReader(spark, parser="lxml")` defaults to `lxml` (fast, lenient). Switch backends with
`.with_parser("html5lib")` for stricter, browser-like parsing of malformed markup.
