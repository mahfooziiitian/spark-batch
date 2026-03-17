---
applyTo: "src/**/*.py"
---

# Python Source Files

## Imports
- Place `from __future__ import annotations` as the first line.
- Group imports: stdlib → third-party → local, separated by blank lines.
- Inside `applyInPandas` UDFs, import `prophet` locally — not at module level.

## Type Annotations
- Annotate every function parameter and return type.
- Use `pd.DataFrame`, `str`, `int`, `float`, `list[str]` — not `Any`.

## Reproducibility
- Always call `np.random.seed(<int>)` at module level when generating synthetic data.

## Section Headers
Use this exact style for logical sections:
```python
# ── SECTION NAME ─────────────────────────────────────────────────────────────
```

## Error Handling
- In UDFs, return an empty DataFrame with the correct schema rather than raising:
  ```python
  if len(group_df) < MIN_ROWS:
      return pd.DataFrame(columns=[f.name for f in result_schema])
  ```

## Docstrings
- Module-level docstring: list all numbered sections the file covers.
- Function docstrings: one sentence describing what the function does.
- Avoid restating the obvious; comment non-obvious algorithmic choices only.
