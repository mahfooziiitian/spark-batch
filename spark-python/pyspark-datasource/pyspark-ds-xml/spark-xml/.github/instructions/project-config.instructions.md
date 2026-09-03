---
applyTo: "pyproject.toml,uv.lock,.python-version"
---

# Project Configuration Conventions

## Package Manager

This project uses **[uv](https://docs.astral.sh/uv/)**. It is a member of the
`pyspark-ds-xml` uv **workspace** — the authoritative lockfile is the workspace
root `pyspark-ds-xml/uv.lock`, not a per-member lock.

```bash
uv sync             # install runtime + dev dependencies
uv run <cmd>        # run a command in the workspace virtual environment
uv add <pkg>        # add a runtime dependency
uv add --dev <pkg>  # add a dev dependency
uv lock             # regenerate the workspace-root lockfile
```

- `[tool.uv] package = false` — this project is a collection of examples plus a
  small `src/spark_xml` helper package; it is **not** built/published as a wheel.

## Dependencies

```toml
[project]
name = "spark-xml"
requires-python = ">=3.11"
dependencies = [
    "pyspark>=4.0.0",   # native "xml" source + from_xml/schema_of_xml (Spark 4)
    # helpers: chardet, faker, pandas, requests, lxml, xmlschema, xmltoxsd, ...
]

[dependency-groups]
dev = ["black", "isort", "flake8-pyproject", "ipykernel"]
```

!!! note "Spark 4 across the board"
    All three sub-projects (`spark-xml`, `spark-xml-etree`, `spark-xpath`) now
    require `pyspark>=4.0.0`. This project in particular depends on the built-in
    `xml` data source, a Spark 4 feature. Do **not** downgrade to `<4.0.0`.

## Tooling Config

- `[tool.black]` and `[tool.isort]` (`profile = "black"`) — line length 120.
- `known_first_party = ["spark_xml"]` — the library package under `src/`.
- `[tool.flake8]` via `flake8-pyproject`; ignores `E203,W503,E402`.
- Run linters from **within** `spark-xml/` so the `[tool.*]` configs are picked
  up from this `pyproject.toml`.

## Rules

- Keep runtime deps in `[project.dependencies]`; keep tooling in
  `[dependency-groups] dev`.
- After changing dependencies, run `uv lock` and commit the updated
  workspace-root lockfile.
