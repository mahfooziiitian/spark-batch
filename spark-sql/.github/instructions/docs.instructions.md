---
applyTo: "docs/**/*.md,mkdocs.yml"
---

# Documentation — MkDocs Material

## Build & Serve

```bash
uv run task docs_build   # strict mode — zero warnings
uv run task docs_serve   # localhost:8080
```

## Navigation

- Every `docs/` directory **must** have a `.pages` file with a `title:` and `nav:`.
- **Never** add `nav:` to `mkdocs.yml` — awesome-pages plugin owns navigation.
- Top-level tabs are defined in `docs/.pages`.

## Page Template

```markdown
# :material-xxx: Title

One-sentence description.

---

## :material-code-tags: Syntax

## :material-information-outline: Behavior

## :material-flask-outline: Practical Examples

## :material-lightbulb-outline: When to Use
```

Separate major sections with `---`.

## Rules

1. Only `:material-xxx:` icons — **no Unicode emoji**.
2. SQL fences: ` ```sql ` (lowercase).
3. Links: relative paths to `.md` files — never to directories, never absolute URLs.
4. Admonitions: `!!! tip`, `!!! note`, `!!! warning`, `!!! success`, `!!! failure`.
5. Tables: GFM pipe syntax.

## Interactive Visualizations (D3.js)

- Place `<div id="viz-<name>" class="ts-viz"></div>` in an `## :material-animation-play: Interactive Demo` section.
- Use `document$.subscribe(init)` for instant-navigation compatibility.
- D3 v7 loaded via CDN before custom JS in `mkdocs.yml`.

## Common Build Fixes

| Warning | Fix |
|---------|-----|
| Unrecognized relative link | Count `../` depth carefully |
| Directory-style link | Append `/index.md` |
| Omitted file | Add to directory's `.pages` |
