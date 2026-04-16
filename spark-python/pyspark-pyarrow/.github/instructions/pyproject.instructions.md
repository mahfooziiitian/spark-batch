---
applyTo: "{**/pyproject.toml,**/poetry.lock}"
---

# pyproject.toml / Poetry Instructions

## Project Metadata

Every sub-project must have a complete `[tool.poetry]` section:

```toml
[tool.poetry]
name = "pyspark-module-name"
version = "0.1.0"
description = "Brief one-line description of the module"
authors = ["mahfooz_iiitian <mahfooz.iiitian@gmail.com>"]
readme = "README.md"
```

## Python Version

Target Python 3.11 as primary, support 3.8+:

```toml
[tool.poetry.dependencies]
python = "^3.11"
```

## PySpark Dependency

Always include the `sql` extra:

```toml
pyspark = {extras = ["sql"], version = "^3.5.1"}
```

## Dev Dependencies

Always include `pytest` and `pytest-mock` in the dev group:

```toml
[tool.poetry.group.dev.dependencies]
pytest = ">=7.0"
pytest-mock = "^3.14.0"
```

## pytest Configuration

Configure pytest inside `pyproject.toml` — no separate `pytest.ini`:

```toml
[tool.pytest.ini_options]
minversion = "6.0"
addopts = "-ra -q"
pythonpath = ["src"]
testpaths = ["tests"]
```

- Set `pythonpath` so test imports resolve without `PYTHONPATH` hacks.
- If source code lives in a subdirectory (e.g., `src/psa/`), include both:
  `pythonpath = ["src", "src/psa"]`.

## Build System

```toml
[build-system]
requires = ["poetry-core"]
build-backend = "poetry.core.masonry.api"
```

## Lock File

Run `poetry lock` after any dependency change. Commit `poetry.lock` to version control
to ensure reproducible installs.
