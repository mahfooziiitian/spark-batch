---
applyTo: "docs/**/*.md,mkdocs.yml"
---

# Documentation Instructions — MkDocs / Material

## Stack

- **MkDocs** `>=1.6.0,<2` — MkDocs 2.x is **incompatible** with Material theme; do not upgrade.
- **Material for MkDocs** `>=9.7.4,<10`
- **mkdocs-awesome-pages-plugin** — navigation is driven by `.pages` files, not by `nav:` in `mkdocs.yml`.
- Build: `uv run mkdocs build --strict` (strict mode must stay green — zero warnings).
- Serve: `uv run mkdocs serve -a 0.0.0.0:8080`

## Navigation

- Every directory in `docs/` **must** have a `.pages` file that lists `nav:` entries.
- Reference format in `.pages`:
  - Directory: `- Label: subdirectory` (auto-discovers `index.md`)
  - File: `- Label: filename.md`
- Never add a `nav:` block to `mkdocs.yml`; the awesome-pages plugin owns navigation.
- Top-level tabs are controlled by `docs/.pages`.

### .pages example

```yaml
title: Functions
nav:
  - index.md
  - Aggregate: aggregate
  - HOF: hof
  - Lambda: lambda.md
```

## Page Structure Template

Every enhanced page must follow this structure in order:

```markdown
# Page Title

One-sentence intro paragraph.

---

## 📌 Syntax

\```sql
FUNCTION(arg1, arg2, ...)
\```

Parameter table:

| Parameter | Type | Description |
|-----------|------|-------------|

---

## 🔍 Behavior

1. Numbered behavior notes.

---

## 🧪 Practical Examples

Multiple SQL examples with `-- Result:` or comment annotations.

---

## 🧠 When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
```

## Writing Rules

1. **Use standard Markdown only** — no raw HTML except inside `attr_list` blocks.
2. **All SQL code blocks** use ` ```sql ` fences.
3. **Links between docs** must be relative paths (e.g., `../filter/index.md`), never absolute URLs.
4. **Never link to a directory** (e.g., `complextype/`) — always link to a specific `.md` file.
5. **Admonitions** use `!!! tip`, `!!! note`, `!!! warning` syntax.
6. **Tables** use GFM pipe syntax with header separator row.
7. Emoji section headers (`📌`, `🔍`, `🧪`, `🧠`) are consistent across all pages.

## Build Validation

Always run after creating or editing docs:

```bash
NO_MKDOCS_2_WARNING=1 uv run mkdocs build --strict
```

Fix all warnings before committing. Common warnings:
- Unrecognized relative link → check directory depth
- Link not found → verify target file exists
- `complextype/` style directory link → change to a file link

## Cross-reference Depth

Relative path levels from common locations:

| From | To `docs/function/` | To `docs/types/` |
|------|---------------------|-----------------|
| `docs/filter/complex/array.md` | `../../function/` | `../../types/` |
| `docs/types/datatype/complextype/arrays/` | `../../../../function/` | `../../../` |
| `docs/dml/table/` | `../../function/` | `../../types/` |
