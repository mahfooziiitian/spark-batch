---
applyTo: "{**/pyproject.toml,**/uv.lock}"
---

# Project Structure — pyspark-pytest

## pyproject.toml Is the Single Source of Truth

All configuration lives in `pyproject.toml`:
- **Project metadata** — `[project]` (PEP 621)
- **Dependencies** — `[project].dependencies` and `[dependency-groups].dev`
- **pytest** — `[tool.pytest.ini_options]`
- **ruff / mypy** — `[tool.ruff]`, `[tool.mypy]`
- **taskipy** — `[tool.taskipy.tasks]`

Do not create standalone config files (`.flake8`, `mypy.ini`, `setup.cfg`, etc.).

## Dependency Management

Use **uv** for all dependency operations:

```bash
uv sync                               # install all deps (runtime + dev)
uv add <package>                      # add a runtime dependency
uv add --group dev <package>          # add a dev dependency
uv lock                               # regenerate uv.lock
```

## Source & Test Layout

```
src/                          ← pythonpath (configured in pytest)
  data_processing.py          ← pipeline modules
  reader/
    spark_reader.py
  transformation/
    df_transformation.py
  utility/
    *.py                      ← Faker data generators

tests/                        ← testpaths (configured in pytest)
  conftest.py                 ← shared SparkSession fixture
  test_data_processing.py
  dataframe/
    test_dataframe.py
    test_df_equality.py
  reader/
    test_spark_reader.py
  spark_context/
    test_spark_context.py
  transformation/
    test_df_transformation.py
```

**Rules:**
- Test directory mirrors source directory.
- One test file per source module.
- Shared fixtures in `tests/conftest.py` only — never in individual test files.
- `__init__.py` files in all source packages.

## Adding a New Module

1. Create source file: `src/<domain>/<module>.py`
2. Ensure `src/<domain>/__init__.py` exists.
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

## pytest Configuration

```toml
[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests", "integration"]
addopts = "-ra -q"
```
