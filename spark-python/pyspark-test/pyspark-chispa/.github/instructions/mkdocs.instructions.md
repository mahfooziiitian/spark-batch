---
applyTo: "{**/mkdocs.yml,**/docs/**/*.md}"
---

# MkDocs Documentation Instructions

## Theme & Config

Use **MkDocs Material** with the project's standard palette:

```yaml
theme:
  name: material
  palette:
    - scheme: default
      primary: deep orange
      accent: orange
      toggle:
        icon: material/brightness-7
        name: Switch to dark mode
    - scheme: slate
      primary: deep orange
      accent: orange
      toggle:
        icon: material/brightness-4
        name: Switch to light mode
  features:
    - navigation.tabs
    - navigation.sections
    - navigation.expand
    - navigation.top
    - search.highlight
    - search.suggest
    - content.code.copy
    - content.code.annotate
```

## Required Extensions

```yaml
markdown_extensions:
  - admonition
  - attr_list
  - md_in_html
  - tables
  - pymdownx.details
  - pymdownx.highlight:
      anchor_linenums: true
      line_spans: __span
      pygments_lang_class: true
  - pymdownx.inlinehilite
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

### Snippet include (preferred — keeps docs in sync with source)

````markdown
```python title="src/data_frame/columns/column_equality.py"
--8<-- "src/data_frame/columns/column_equality.py"
```
````

### Code annotations

```python
spark = (
    SparkSession.builder.master("local[*]")  # (1)!
    .config("spark.sql.shuffle.partitions", "4")  # (2)!
    .getOrCreate()
)
```
1. Use all available CPU cores.
2. Default 200 is too high for small local datasets.

### Tabbed install options

````markdown
=== "pip"
    ```bash
    pip install pyspark chispa pytest
    ```

=== "uv"
    ```bash
    uv add pyspark chispa pytest
    ```
````

## Admonitions

```markdown
!!! tip "Quick start"
    Run `uv run task test` to execute all tests.

!!! warning "Java required"
    Java 11 or 17 must be on your `PATH` for PySpark.

!!! note
    chispa assertion errors produce rich diff tables.
```

## Page Structure

1. **Short description** — what this page covers.
2. **Prerequisites** — dependencies, install commands.
3. **Code example** — with annotations.
4. **Run the example** — exact command.
5. **Test example** — corresponding test with chispa.
6. **Reference** — links, config tables.

## Building & Serving

```bash
uv run task docs        # mkdocs build --strict
uv run task docs_serve  # mkdocs serve (live reload)
```
