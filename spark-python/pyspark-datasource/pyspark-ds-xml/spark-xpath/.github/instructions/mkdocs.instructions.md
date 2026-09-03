---
applyTo: "docs/**/*.md,mkdocs.yml"
---

# MkDocs Documentation Conventions

## Stack

- **MkDocs** ≥ 1.6 with **mkdocs-material** ≥ 9.5 theme.
- Config file: `mkdocs.yml` at project root.
- Source files: `docs/` directory (Markdown).

## Site Structure

```
docs/
├── index.md               # Home / landing page
├── getting-started.md     # Installation & first steps
├── xpath-functions.md     # XPath function reference
├── testing.md             # Testing guide
└── examples/
    ├── basic-parsing.md
    ├── nested-xml.md
    └── credit-evaluation.md
```

## Adding a New Page

1. Create a `.md` file in the appropriate `docs/` subdirectory.
2. Register it in `mkdocs.yml` under the `nav:` section:
   ```yaml
   nav:
     - Examples:
         - My New Example: examples/my-new-example.md
   ```
3. Build and verify: `uv run mkdocs build --strict`.

## Markdown Extensions Available

These are configured in `mkdocs.yml` — use them freely:

### Admonitions

```markdown
!!! tip "Title"
    Tip content here.

!!! warning
    Warning content here.

!!! note
    Note content here.

!!! info
    Info content here.
```

### Code Blocks with Syntax Highlighting

````markdown
```python
from pyspark.sql import SparkSession
```

```sql
SELECT xpath_string(data, 'Root/Child') FROM xml_data
```

```bash
uv run pytest tests/ -v
```
````

### Tabbed Content

```markdown
=== "Python"

    ```python
    spark.sql("SELECT xpath_string(data, 'Root/Child') FROM xml_data")
    ```

=== "SQL"

    ```sql
    SELECT xpath_string(data, 'Root/Child') FROM xml_data
    ```
```

## Writing Style

- Use **second person** ("you") when addressing the reader.
- Keep paragraphs short — 2–4 sentences max.
- Use tables for structured comparisons (functions, options, configs).
- Include runnable code examples in every page.
- Always show **expected output** after code examples where practical.
- Reference source files with relative paths: `examples/xml_xpath.py`.

## Example Page Template

```markdown
# Page Title

Brief description of what this page covers.

## Source

:material-file-code: `examples/<file>.py`

## The XML

\`\`\`xml
<Root><Child>value</Child></Root>
\`\`\`

## Code Walkthrough

### Step 1 — Create the DataFrame
\`\`\`python
# code here
\`\`\`

### Step 2 — Run the XPath Query
\`\`\`sql
SELECT xpath_string(data, 'Root/Child') FROM xml_data
\`\`\`

## Expected Output

| column | value |
| ------ | ----- |
| child  | value |
```

## Building & Serving

```bash
uv run mkdocs serve              # local dev server with hot-reload (http://127.0.0.1:8000)
uv run mkdocs build --strict     # production build — fails on warnings (used in CI)
```

## CI Integration

The GitHub Actions workflow builds docs with `--strict` after tests pass.
Any broken links, missing pages, or config errors will fail the build.
