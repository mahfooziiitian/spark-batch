---
applyTo: "{**/pyproject.toml,**/poetry.lock}"
---

# Project Structure — pyspark-pytest

## pyproject.toml Is the Single Source of Truth

All configuration lives in `pyproject.toml`:
- **Project metadata** — `[tool.poetry]`
- **Dependencies** — `[tool.poetry.dependencies]` and `[tool.poetry.group.dev.dependencies]`
- **pytest** — `[tool.pytest.ini_options]`
- **Build system** — `poetry-core`

Do not create standalone config files (`.flake8`, `mypy.ini`, `setup.cfg`, etc.).

## Dependency Management

Use **poetry** for all dependency operations:

```bash
poetry install                    # install all deps
poetry add <package>              # add a runtime dependency
poetry add --group dev <package>  # add a dev dependency
poetry lock                       # regenerate poetry.lock
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
5. Run `poetry run pytest` to validate.

## pytest Configuration

```toml
[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests", "integration"]
addopts = "-ra -q"
```
