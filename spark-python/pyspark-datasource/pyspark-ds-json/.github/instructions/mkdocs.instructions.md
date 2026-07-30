---
applyTo: "{**/mkdocs.yml,**/docs/**/*.md}"
---

# Documentation Instructions (MkDocs Material)

## Theme & Configuration

This project uses **MkDocs Material** with deep orange palette, dark/light toggle,
and full navigation features. Configuration lives in `mkdocs.yml` at the project root.

### Required features

```yaml
theme:
  name: material
  palette:
    - scheme: default
      primary: deep orange
      accent: orange
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
    - content.tabs.link
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

### Source inclusion (preferred — keeps docs in sync)

Always include Python examples via snippet directives:

```markdown
```python title="examples/06_schema/01_struct_type_schema.py"
--8<-- "examples/06_schema/01_struct_type_schema.py"
`` `
```

### Tabbed install options

````markdown
=== "uv (Recommended)"
    ```bash
    uv add pyspark
    ```

=== "pip"
    ```bash
    pip install pyspark>=4.0.0
    ```
````

### Code annotations

```python
spark = get_spark("my-app")  # (1)!
```
1. Calls `configure_env()` internally — sets JAVA_HOME and PYSPARK_PYTHON.

## Admonitions

Use admonitions for callouts:

```markdown
!!! tip "PySpark 4 Feature"
    VARIANT type and Spark Connect are available in PySpark 4+.

!!! warning "Java 17 Required"
    PySpark 4.x requires Java 17 or later.

!!! note
    Set `PYS_JSON_LOG_LEVEL=DEBUG` for verbose library output.

!!! success "When to use"
    - Explicit schemas for production JSON pipelines
    - Rich output for interactive development

!!! failure "Avoid"
    - Schema inference on large production datasets
    - Hardcoded file paths
```

## Mermaid Diagrams

Use for architecture, data flow, and decision trees:

```markdown
```mermaid
graph LR
    JSON[JSON File] -->|spark.read.json| DF[DataFrame]
    DF -->|from_json| Parsed[Structured Column]
    DF -->|to_json| Output[JSON String]
`` `
```

## Page Structure

Every documentation page should follow this order:

1. **Title and description** — what this topic covers
2. **Diagram** (mermaid) — visual overview when applicable
3. **Key concepts** — bullet list of what the reader will learn
4. **Code example** — snippet include from `examples/`
5. **Run section** — exact command to execute
6. **Configuration table** — relevant Spark options
7. **Tips/warnings** — admonitions for gotchas

## File Naming

- Use kebab-case for doc files: `schema-inference.md`, `drop-malformed.md`
- Match nav structure to folder structure under `docs/`
- Keep file names short but descriptive

## Snippet Paths

All snippet includes are relative to project root. Pattern:

```
examples/<category>/<filename>.py
src/pys_json/<module>/<file>.py
```

After renaming/moving example files, always update corresponding snippet paths
in `docs/` and verify with `mkdocs build --strict`.

## Build & Verify

```bash
# Build docs (strict mode catches broken links/snippets)
uv run mkdocs build --strict

# Serve locally for preview
uv run mkdocs serve

# Deploy to GitHub Pages
uv run mkdocs gh-deploy
```
