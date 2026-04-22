---
applyTo: "docs/**/*.md,mkdocs.yml"
---

# Documentation Instructions — MkDocs / Material

## Stack

- **MkDocs** `>=1.6.0,<2` — MkDocs 2.x is **incompatible** with Material theme; do not upgrade.
- **Material for MkDocs** `>=9.7.4,<10`
- **mkdocs-awesome-pages-plugin** `>=2.10.1` — navigation driven by `.pages` files, not `nav:` in `mkdocs.yml`.
- **mkdocs-include-markdown-plugin** `>=6.0.0` — for `--8<--` snippet includes.
- Build: `NO_MKDOCS_2_WARNING=1 uv run mkdocs build --strict` (strict mode — zero warnings).
- Serve: `uv run mkdocs serve -a 0.0.0.0:8080`

## Theme Configuration

The project uses **deep purple** / **amber** palette with the following features enabled in `mkdocs.yml`:

```yaml
theme:
  name: material
  features:
    - navigation.tabs
    - navigation.tabs.sticky
    - navigation.sections
    - navigation.indexes
    - navigation.instant
    - navigation.instant.prefetch
    - navigation.tracking
    - toc.follow
    - search.suggest
    - search.highlight
    - content.code.copy
    - content.code.annotate
    - content.tooltips
    - content.tabs.link
```

## Navigation

- Every directory in `docs/` **must** have a `.pages` file.
- Reference format in `.pages`:
  - Directory: `- Label: subdirectory` (auto-discovers `index.md`)
  - File: `- Label: filename.md`
  - Optional `title:` key sets the section heading in the sidebar.
- **Never** add a `nav:` block to `mkdocs.yml` — the awesome-pages plugin owns navigation.
- Top-level tabs are controlled by `docs/.pages`.

### `.pages` example

```yaml
title: SCD Patterns
nav:
  - index.md
  - Introduction: intro.md
  - Type 1: type1
  - Type 2: type2
```

## Icons

- **Only** use Material Design icons: `:material-xxx:` syntax.
- **Never** use Unicode emoji (🔍, 📌, 🧪, etc.) — they render inconsistently across themes.
- Common section icons:

| Purpose | Icon |
|---------|------|
| Syntax / definition | `:material-code-tags:` |
| Behavior / notes | `:material-information-outline:` |
| Examples | `:material-flask-outline:` |
| When to use | `:material-lightbulb-outline:` |
| Table design | `:material-toy-brick:` |
| Step-by-step | `:material-repeat:` |
| Demo / walkthrough | `:material-play-circle:` |
| Warning / pitfalls | `:material-shield-outline:` |
| Interactive viz | `:material-animation-play:` |
| Comparison | `:material-swap-horizontal:` |

## Page Structure Template

Every reference page must follow this order:

```markdown
# :material-xxx: Page Title

One-sentence description — what this is and when it applies.

---

## :material-code-tags: Syntax

\```sql
FUNCTION(arg1, arg2, ...)
\```

Parameter table:

| Parameter | Type | Description |
|-----------|------|-------------|

---

## :material-information-outline: Behavior

1. Numbered behavior notes.

---

## :material-flask-outline: Practical Examples

Multiple SQL examples with `-- Result:` annotations.

---

## :material-lightbulb-outline: When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
```

### Demo / walkthrough pages

Use numbered steps with `:material-numeric-N-circle:` icons:

```markdown
## :material-numeric-1-circle: Step title

Content...

## :material-numeric-2-circle: Next step
```

## Writing Rules

1. **Standard Markdown only** — no raw HTML except inside `attr_list` blocks.
2. **All SQL code blocks** use ` ```sql ` fences with lowercase `sql`.
3. **Links between docs** must be relative paths (e.g. `../filter/index.md`), never absolute URLs.
4. **Never link to a directory** (e.g. `scd/type2/`) — always link to a specific `.md` file.
5. **Admonitions**: `!!! tip`, `!!! note`, `!!! warning`, `!!! success`, `!!! failure`.
6. **Tables** use GFM pipe syntax with a header separator row.
7. **Horizontal rules** (`---`) separate major sections.
8. **No Unicode emoji** in any `.md` file — replace with `:material-xxx:` icons.

## D3.js Interactive Visualizations

Pages that include interactive demos must:

1. Place `<div id="viz-<name>" class="ts-viz"></div>` in an `## :material-animation-play: Interactive Demo` section.
2. The JS entry point in `docs/assets/js/timeseries-viz.js` maps IDs to render functions.
3. Use `document$.subscribe(init)` (Material's RxJS observable) for instant-navigation compatibility.
4. D3 v7 is loaded via CDN in `mkdocs.yml` `extra_javascript` **before** the custom JS file.

```yaml
extra_javascript:
  - https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js
  - assets/js/timeseries-viz.js

extra_css:
  - assets/css/timeseries-viz.css
```

## Build Validation

Always run after creating or editing docs:

```bash
NO_MKDOCS_2_WARNING=1 uv run mkdocs build --strict
```

Common warnings and fixes:

| Warning | Fix |
|---------|-----|
| Unrecognized relative link | Check directory depth — count `../` levels |
| Link not found | Verify target `.md` file exists |
| Directory-style link (`scd/type2/`) | Change to file link (`scd/type2/index.md`) |
| Omitted file | Add entry to the directory's `.pages` file |

## Cross-reference Depth

| From | To `docs/function/` | To `docs/scd/` |
|------|---------------------|---------------|
| `docs/scd/type2/index.md` | `../../function/` | `../` |
| `docs/timeseries/tumbling.md` | `../function/` | `../scd/` |
| `docs/types/datatype/arrays/` | `../../../function/` | `../../../scd/` |
