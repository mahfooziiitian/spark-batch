---
applyTo: "{**/pyproject.toml,**/uv.lock}"
---

# Project Structure — pyspark-deepu

## Dependencies

Use **uv** for all dependency operations:

```bash
uv sync                               # install all deps (runtime + dev)
uv add <package>                      # add a runtime dependency
uv add --group dev <package>          # add a dev dependency
uv lock                               # regenerate uv.lock
```

## pyproject.toml Is the Single Source of Truth

All configuration lives in `pyproject.toml`:
- **Project metadata** — `[project]` (PEP 621)
- **Dependencies** — `[project].dependencies` and `[dependency-groups].dev`
- **pytest** — `[tool.pytest.ini_options]`
- **ruff / mypy** — `[tool.ruff]`, `[tool.mypy]`
- **taskipy** — `[tool.taskipy.tasks]`

Do not create standalone config files (`.flake8`, `mypy.ini`, `setup.cfg`, etc.).

## Source & Test Layout

```
src/                          ← package root (configured in pytest pythonpath)
  constraints/
    suggestions/              ← ConstraintSuggestionRunner scripts
    verifications/            ← VerificationSuite scripts
  mertics/
    computations/
      analyzers/              ← AnalysisRunner scripts
      profiles/               ← ColumnProfilerRunner scripts
    repository/               ← FileSystemMetricsRepository scripts

tests/                        ← pytest test root
  conftest.py                 ← shared SparkSession fixture with Deequ JAR config
  mertics/computations/
    test_analyzers.py
```

**Rules:**
- Test directory mirrors source directory.
- One test file per source module.
- `__init__.py` files in all source packages.
- Shared fixtures in `tests/conftest.py` only — never in individual test files.

## Adding a New Module

1. Create source file: `src/<domain>/<module>.py`
2. Ensure `__init__.py` exists in all parent directories.
3. Create test file: `tests/<domain>/test_<module>.py`
4. Import and use the shared `spark` fixture from `conftest.py`.
5. Run `uv run task test` to validate.

## Common Tasks

```bash
uv run task test          # run tests
uv run task lint          # lint with ruff
uv run task format        # format with ruff
uv run task typecheck     # type check with mypy
uv run task docs          # build MkDocs site
uv run task docs_serve    # serve docs locally
uv run task check         # lint + format check + test
```
