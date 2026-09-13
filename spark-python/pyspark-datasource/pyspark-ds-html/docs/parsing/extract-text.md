# Extracting Text

`strip_html_tags()` removes all markup, leaving visible text. `extract_tag_text()` targets a
specific tag (e.g., `h1`, `p`, `li`) and returns an `array<string>` of matches.

```python
--8<-- "examples/02_parsing/02_extract_text.py"
```
