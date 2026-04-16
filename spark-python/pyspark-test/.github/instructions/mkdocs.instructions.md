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
  features:
    - navigation.tabs
    - navigation.sections
    - navigation.expand
    - navigation.top
    - search.highlight
    - content.code.copy
    - content.code.annotate
```

## Markdown Extensions

Always include: admonition, tabbed, superfences (mermaid), snippets, highlight, toc.

## Code Blocks

Prefer snippet includes to keep docs in sync with source:

```markdown
```python title="src/my_module.py"
--8<-- "src/my_module.py"
```
```

## Admonitions

```markdown
!!! tip "Title"
    Helpful tip content.

!!! warning "Title"
    Warning content.
```

## Page Structure

1. Short description
2. Prerequisites (tabbed pip / conda / uv)
3. Code snippet with annotations
4. Run instructions
5. Configuration reference table
