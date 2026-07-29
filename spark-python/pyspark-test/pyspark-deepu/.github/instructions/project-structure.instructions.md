---
applyTo: "**/pyproject.toml"
---

# Project Structure — pyspark-deepu

## Package Configuration

The project uses `pyproject.toml` for all configuration — dependencies, pytest,
ruff, mypy, and taskipy task definitions. Managed by **uv**.

### Key pyproject.toml sections

- `[project]` — name, version, Python requirement, runtime deps (pyspark, pydeequ)
- `[dependency-groups]` — dev group (pytest, ruff, mypy, taskipy, mkdocs)
- `[tool.pytest.ini_options]` — pythonpath=src, testpaths=tests
- `[tool.ruff]` — line-length=100, target-version=py311
- `[tool.mypy]` — ignore_missing_imports=true
- `[tool.taskipy.tasks]` — test, lint, format, typecheck, check, docs, clean

## Dependency Management

Use **uv** for all dependency operations:

```bash
uv sync --group dev                        # install all dependencies
uv add <package>                           # add a runtime dependency
uv add --group dev <package>               # add a dev dependency
```

## Source & Test Layout

```
src/                          ← package root (in pythonpath)
  constraints/
    suggestions/
      constraint_suggestions.py
    verifications/
      constraint_verification.py
  mertics/
    computations/
      analyzers/
        analyzers.py
      profiles/
        mertics_profile.py
    repository/
      repository.py

tests/                        ← pytest test root
  conftest.py                 ← shared SparkSession + Deequ fixture
  constraints/
    test_verification.py
    test_suggestions.py
  mertics/computations/
    test_analyzers.py
    test_profiling.py
```

**Rules:**
- Test directory mirrors source directory.
- One test file per source module.
- `__init__.py` files in all source packages.
- Shared `conftest.py` in `tests/` — do NOT add per-file fixtures.

## Adding a New Module

1. Create source file: `src/<domain>/<module>.py`
2. Ensure `__init__.py` exists in all parent directories.
3. Create test file: `tests/<domain>/test_<module>.py`
4. Use the shared `spark` fixture from `conftest.py`.
5. Run `uv run pytest tests/<domain>/test_<module>.py -v` to validate.

## Running Tasks

```bash
uv run task test                           # run tests
uv run task lint                           # lint with ruff
uv run task format                         # format with ruff
uv run task typecheck                      # type check with mypy
uv run task check                          # full CI pipeline
uv run task docs                           # build docs
uv run task docs_serve                     # serve docs locally
uv run task clean                          # remove caches
```
