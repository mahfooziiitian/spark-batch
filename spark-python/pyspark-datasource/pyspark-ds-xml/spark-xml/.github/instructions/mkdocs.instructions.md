---
applyTo: "docs/**/*.md,mkdocs.yml"
---

# MkDocs Documentation Conventions

## Stack

- **MkDocs** ≥ 1.6 with the **mkdocs-material** ≥ 9.5 theme.
- This project is a member of the `pyspark-ds-xml` monorepo docs site
  (aggregated via `mkdocs-monorepo-plugin`). It also builds standalone from its
  own `mkdocs.yml`.

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
    - navigation.top
    - navigation.indexes
    - search.highlight
    - search.suggest
    - content.code.copy
    - content.code.annotate
```

## Markdown Extensions

- `admonition`, `attr_list`, `md_in_html`, `tables`
- `pymdownx.details`, `pymdownx.inlinehilite`, `pymdownx.highlight`
- `pymdownx.superfences` with a `mermaid` custom fence
- `pymdownx.tabbed` (`alternate_style: true`)
- `toc` with `permalink: true`

## Referencing Source Files

Point readers at the correct folder for the code being shown:

- **Example / demo scripts** live in `examples/` — reference as
  `> **Source:** \`examples/<feature>/<script>.py\``.
- **Library / helper / utility** code lives in `src/spark_xml/` — reference as
  `> **Source:** \`src/spark_xml/util/<script>.py\``.

## Admonitions

```markdown
!!! tip "No JAR required"
    The `xml` data source is built into Spark 4 — no `spark.jars.packages`.

!!! warning "Spark 4 required"
    `from_xml` / `schema_of_xml` and `format("xml")` require PySpark ≥ 4.0.
```

## Page Structure

1. Description — what the pattern does and when to use it.
2. Data flow diagram (Mermaid) for complex transformations.
3. Prerequisites — `uv sync`.
4. Source code reference (`examples/...` or `src/spark_xml/...`).
5. Run command (`uv run python examples/<feature>/<script>.py`).
6. Expected output (truncated `show()` / `printSchema()`).
7. When to use / not use (`!!! success` / `!!! failure`).

## Build & Serve

```bash
uv run mkdocs serve          # local preview at http://127.0.0.1:8000
uv run mkdocs build --strict # production build — fails on warnings
```
