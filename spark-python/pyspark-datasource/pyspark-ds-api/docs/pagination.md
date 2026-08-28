# Pagination

Pagination strategies live under `examples/paginated/`, each pairing a mock
API server with an ETL script that drives `rest_ds.rest_api.APIClient` /
`PaginationFactory`.

| Strategy | Example path |
|---|---|
| Cursor-based | `examples/paginated/cursor/` |
| Offset — simple | `examples/paginated/offset/simple/` |
| Offset — page token | `examples/paginated/offset/page_token/` |
| Page number | `examples/paginated/page_number/` (multiple source variants) |

## How it fits together

`rest_ds.rest_api.PaginationFactory.get_paginator()` returns the concrete
`Paginator` subclass for a given `strategy` name from YAML config
(`options.pagination.strategy`). Each `Paginator` implementation knows how to
read the next-page token/cursor/offset out of the previous response and stop
when the API signals there are no more pages.

```yaml
options:
  pagination:
    strategy: cursor        # or: offset_simple, offset_page_token, page_number
    # ...strategy-specific fields (cursor_field, limit, result_key, etc.)
```

!!! note "Known limitation"
    A failure on any single page currently aborts the whole paginated fetch —
    there is no partial-page retry/resume yet (tracked in the project
    root `README.md` Limitations section).

## Library reference

See [`rest_ds.rest_api`][rest_ds.rest_api] in the
[API reference](reference.md) for `APIClient`, the `Paginator` hierarchy, and
`FileWriter`.
