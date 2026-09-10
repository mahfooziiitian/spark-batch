---
applyTo: "pyproject.toml,Makefile,justfile,.github/**,src/**,sql/**,tests/**"
---

# Quality & CI

## Task Runner

The **Makefile is the primary interface** — run `make help` to list every target.
On Windows (or any platform), the **`justfile`** mirrors the same targets: run
`just <target>` (requires [`just`](https://just.systems)). An older `taskipy`
mirror (`uv run task <name>`, in `[tool.taskipy.tasks]`) still exists but has
drifted (e.g. it still calls the deprecated `safety check`); **prefer `make`/`just`**.

### Key Commands

| Makefile          | taskipy (legacy)         | Purpose                            |
|-------------------|--------------------------|------------------------------------|
| `make quality`    | `uv run task quality`    | format → lint → type-check → sql   |
| `make test-fast`  | `uv run task test_fast`  | pytest, stop on first failure      |
| `make test-cov`   | `uv run task test_cov`   | pytest + coverage gate             |
| `make docs-build` | `uv run task docs_build` | MkDocs strict build                |
| `make secure`     | `uv run task secure`     | bandit + `safety scan`             |
| `make ci`         | `uv run task ci`         | non-mutating full pipeline         |

## Pre-commit Gate

```bash
make pre-commit   # quality + docs-build + test
```

## Configuration

Tool config lives in `pyproject.toml`. **Do not** create redundant configs
(`.flake8`, `setup.cfg`, `.mypy.ini`, `.isort.cfg`, `ruff.toml`). A few tools
legitimately need their own file — leave these in place:

| File                 | Owned by | Purpose                                              |
|----------------------|----------|------------------------------------------------------|
| `.sqlfluffignore`    | sqlfluff | Excludes Databricks-only `.sql` from `sparksql` lint |
| `.safety-policy.yml` | safety   | v3 `scan` policy (severity gate + report settings)   |
| `.pages`             | mkdocs   | Per-directory navigation                             |

| Setting          | Value                                                                        |
|------------------|------------------------------------------------------------------------------|
| Max line length  | 128                                                                          |
| Python target    | 3.11 (`requires-python >=3.11,<3.13`)                                        |
| SQL dialect      | `sparksql` (open-source Spark 4)                                             |
| Coverage minimum | 60% — scoped to the tested SQL helper; `dbx_mcp`/`util`/`model` are omitted  |
| Ruff rules       | `E, F, W, I, UP, B, SIM, TCH, RUF`                                           |

## Dependency Management

```bash
uv add <package>              # runtime  (keep runtime deps minimal)
uv add --group dev <package>  # dev / docs / tooling
```

- Always commit `uv.lock`.
- Pin breaking majors (e.g. `mkdocs>=1.6,<2`, `mcp[cli]>=1.0.0,<2.0.0`).
- **Runtime vs dev:** runtime deps are only what the `spark_sql` package imports
  (`pyspark`, `databricks-sdk`, `mcp[cli]`, `pydantic-settings`, `python-dotenv`).
  Linters, docs, and notebook tooling belong in the `dev` group.

## Security

- `make bandit` scans `src/` only (config in `pyproject.toml`).
  Suppress a false positive with `# nosec <ID>` **plus a justification**
  (e.g. `# nosec B105 - env var name placeholder, not a secret`).
- `make safety` runs `safety scan` (v3), which authenticates via the
  `SAFETY_API_KEY` environment variable and reads `.safety-policy.yml`.
  The gate passes when the declared `pyproject.toml` deps are clean.
- Never commit secrets.
