---
applyTo: "docs/**/*.md,mkdocs.yml"
---

# MkDocs Documentation Conventions

## Stack

- MkDocs ≥ 1.6 with **mkdocs-material** ≥ 9.5.
- Markdown extensions: admonitions, tabbed content, code annotations, Mermaid diagrams.

## Theme Configuration

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
    - navigation.indexes
    - search.highlight
    - search.suggest
    - content.code.copy
    - content.code.annotate
```

## Required Plugins and Extensions

```yaml
plugins:
  - search
  - include-markdown

markdown_extensions:
  - admonition
  - attr_list
  - md_in_html
  - tables
  - pymdownx.details
  - pymdownx.inlinehilite
  - pymdownx.highlight:
      anchor_linenums: true
      line_spans: __span
      pygments_lang_class: true
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

### Include source files (preferred — keeps docs in sync)

````markdown
```python title="src/text/read/read_text_basic.py"
--8<-- "src/text/read/read_text_basic.py"
```
````

### Code annotations

```python
spark = (
    SparkSession.builder
    .master(os.environ.get("SPARK_MASTER", "local[*]"))  # (1)!
    .config("spark.ui.enabled", "false")                  # (2)!
    .getOrCreate()
)
```

1. Falls back to local mode when `SPARK_MASTER` is not set.
2. Skip the Spark Web UI for faster startup.

### Tabbed install options

````markdown
=== "pip"
    ```bash
    pip install pyspark
    ```

=== "uv"
    ```bash
    uv add pyspark
    ```
````

## Admonitions

```markdown
!!! tip "No external JARs needed"
    The text datasource is built into PySpark — no additional dependencies required.

!!! warning "Single column only"
    `df.write.text()` requires exactly one `StringType` column. Concatenate
    fields with `F.concat_ws()` before writing.

!!! note
    Spark auto-detects gzip and bzip2 compression by file extension.
```

## Architecture Diagrams

Use Mermaid for data flow:

````markdown
```mermaid
graph LR
    A[Text File] -->|spark.read.text| B[DataFrame: value column]
    B -->|F.split / F.regexp_extract| C[Structured Columns]
    C -->|createOrReplaceTempView| D[SQL Queries]
    C -->|F.concat_ws| E[df.write.text]
```
````

## Read Options Diagram

````markdown
```mermaid
graph TD
    A[spark.read] --> B[.text path]
    A --> C[.option wholetext true]
    A --> D[.option lineSep ;]
    A --> E[.option encoding ISO-8859-1]
    A --> F[.option pathGlobFilter *.log]
    A --> G[.option recursiveFileLookup true]
    A --> H[.option compression gzip]
```
````

## Page Structure

Each example page should follow this order:

1. **Description** — what this pattern does and when to use it.
2. **Data flow diagram** (Mermaid) — for complex transformations.
3. **Prerequisites** — `uv sync` and any config needed.
4. **Read options table** — for reader-focused pages.
5. **Source code** — `--8<--` snippet include with annotations.
6. **Run the example** — bash command block.
7. **Expected output** — truncated `show()` output.
8. **When to use / not use** — `!!! success` and `!!! failure` admonitions.

## Build and Serve

```bash
uv run mkdocs serve          # local preview at http://127.0.0.1:8000
uv run mkdocs build --strict # production build — fails on warnings
```
