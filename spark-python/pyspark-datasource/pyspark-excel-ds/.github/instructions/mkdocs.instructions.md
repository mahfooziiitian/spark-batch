---
applyTo: "{mkdocs.yml,docs/**/*.md}"
---

# Documentation Instructions (MkDocs Material)

## Theme & Configuration

This project uses **MkDocs Material** with a **green / light green** palette
(distinct from other sibling datasource projects), dark/light toggle, and
full navigation features. Configuration lives in `mkdocs.yml` at the project
root.

### Required features

```yaml
theme:
  name: material
  palette:
    - scheme: default
      primary: green
      accent: light green
    - scheme: slate
      primary: green
      accent: light green
  features:
    - navigation.tabs
    - navigation.sections
    - navigation.expand
    - navigation.footer
    - navigation.top
    - navigation.indexes
    - search.highlight
    - search.suggest
    - content.code.copy
    - content.code.annotate
```

### Required extensions

```yaml
markdown_extensions:
  - admonition
  - attr_list
  - md_in_html
  - tables
  - pymdownx.details
  - pymdownx.highlight:
      anchor_linenums: true
  - pymdownx.superfences:
      custom_fences:
        - name: mermaid
          class: mermaid
          format: !!python/name:pymdownx.superfences.fence_code_format
  - pymdownx.tabbed:
      alternate_style: true
  - pymdownx.snippets:
      base_path: ["."]
  - toc:
      permalink: true
```

## Code Blocks

### Tabbed install options

````markdown
=== "uv (Recommended)"
    ```bash
    uv sync --group dev
    ```

=== "pip"
    ```bash
    pip install -e ".[delta]"
    ```
````

### Code annotations

```python
spark = get_spark("my-app")  # (1)!
```
1. Calls `configure_env()` internally — sets up the sample data directory and
   optionally the Delta Lake extension.

## Admonitions

```markdown
!!! tip "Databricks Runtime 17.1+"
    The built-in `excel` format needs no library install.

!!! warning "DBR 15.x / 16.x"
    Attach `com.crealytics:spark-excel_2.12:3.5.1_0.20.4` as a cluster Maven
    library — Excel is not built in on these runtimes.

!!! note
    Set `PYS_EXCEL_LOG_LEVEL=DEBUG` for verbose library output.

!!! success "When to use"
    - The pandas bridge for reports and small extracts
    - spark-excel for cluster-scale ingestion

!!! failure "Avoid"
    - Hardcoding the `"excel"` format string outside `resolve_excel_format()`
    - Making `delta-spark` a hard dependency
```

## Mermaid Diagrams

```markdown
```mermaid
graph LR
    A[Excel Extract] -->|excel_to_table| B[(Spark Table)]
    B -->|table_to_excel| C[Excel Report]
`` `
```

## Page Structure

Every documentation page should follow this order:

1. **Title and description** — what this topic covers
2. **Diagram** (mermaid) — when applicable
3. **Code example(s)** — using real `pys_excel` APIs, matching the actual
   function signatures in `src/pys_excel/`
4. **Options/parameters table** — when documenting a reader/writer/table
   function
5. **Tips/warnings** — admonitions for gotchas (Delta optionality, Databricks
   runtime version differences, xlsxwriter-vs-openpyxl feature gaps, etc.)
6. **Cross-links** to related pages (e.g. reading ↔ schema ↔ properties)

## File Naming

- Use kebab-case for doc files: `spark-excel-library.md`, `upsert-merge.md`.
- Match nav structure to folder structure under `docs/`.

## Nav Structure

`mkdocs.yml` nav covers: Home, Getting Started, Data Source (+ spark-excel
library page), Table Integration, Properties, Schema, Error Handling,
Databricks, Best Practices. When adding a new doc page, add it to the nav in
the same response.

## Build & Verify

```bash
uv run mkdocs build --strict   # Catches broken links/nav entries
uv run mkdocs serve            # Local preview
make docs                      # Wraps mkdocs build --strict
make docs-lint                 # pymarkdownlnt over docs/
```
