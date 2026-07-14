---
applyTo: "{**/mkdocs.yml,**/docs/**/*.md}"
---

# MkDocs Documentation Instructions

## Theme & Config (mkdocs.yml)

Use **MkDocs Material** with the project's standard palette and feature set:

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

Always include these plugins:
```yaml
plugins:
  - search
  - include-markdown
```

Always include these markdown extensions:
```yaml
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

### Snippet include (preferred — keeps docs in sync with source)
```markdown
```python title="src/spp/pandas_udf/pandas_udf.py"
--8<-- "src/spp/pandas_udf/pandas_udf.py"
```
```

### Tabbed install options
````markdown
=== "pip"
    ```bash
    pip install pyspark==3.5.0 pandas pyarrow
    ```

=== "conda"
    ```bash
    conda install -c conda-forge pyspark=3.5.0 pandas pyarrow
    ```

=== "uv"
    ```bash
    uv add pyspark pandas pyarrow
    ```
````

### Code annotations
```python
spark = (SparkSession.builder
         .master("local[*]")                          # (1)!
         .config("spark.sql.shuffle.partitions", "4") # (2)!
         .getOrCreate())
```
1. Use all available CPU cores.
2. Default 200 is too high for small local datasets.

## Admonitions

```markdown
!!! tip "No cluster needed"
    Start with local mode — it runs on your laptop in seconds.

!!! warning "Java required"
    Java 8, 11, or 17 must be on your `PATH`.

!!! note
    Enable Arrow for optimal pandas ↔ Spark performance.

!!! success "Good fit"
    - Pandas UDFs for vectorized operations
    - Pandas API on Spark for familiar syntax

!!! failure "Not a good fit"
    - Pure pandas on data larger than driver memory
```

## Page Structure

Every topic page should follow this order:

1. **Short description** — what this feature is and when to use it.
2. **Prerequisites** — tabbed pip / conda / uv install blocks + Java warning.
3. **SparkSession snippet** — with code annotations.
4. **Run the example** — bash block with the exact command.
5. **Configuration reference** — table of key Spark configs.
6. **When to use / not use** — `!!! success` and `!!! failure` admonitions.
7. **Full example** — `--8<--` snippet include.
