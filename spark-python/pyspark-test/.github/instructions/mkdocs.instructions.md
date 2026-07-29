---
applyTo: "{**/mkdocs.yml,**/docs/**/*.md}"
---

# MkDocs Documentation Instructions (Root-Level Defaults)

## Theme

Use **MkDocs Material** with deep orange / orange palette:

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

## Markdown Extensions

Always include: admonition, tabbed, superfences (mermaid), snippets, highlight, toc.

## Code Blocks

Prefer snippet includes to keep docs in sync with source:

````markdown
```python title="src/my_module.py"
--8<-- "src/my_module.py"
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

### Tabbed install options

Use tabs when multiple package manager options exist:

````markdown
=== "pip"
    ```bash
    pip install pyspark chispa pytest
    ```

=== "poetry"
    ```bash
    poetry add pyspark chispa pytest
    ```
````

## Admonitions

```markdown
!!! tip "Title"
    Helpful tip content.

!!! warning "Title"
    Warning content.

!!! note
    Informational content.
```

## Page Structure

1. Short description
2. Prerequisites (tabbed pip / poetry / uv when applicable)
3. Code snippet with annotations
4. Run instructions
5. Test example (if applicable)
6. Configuration reference table

## Cross-Referencing

When referencing files from documentation, use relative paths from the docs directory.
When including source files with snippets, paths are relative to `pymdownx.snippets.base_path`.
