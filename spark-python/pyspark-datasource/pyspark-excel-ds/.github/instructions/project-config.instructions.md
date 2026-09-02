---
applyTo: "pyproject.toml"
---

# Project Configuration

## Build System

This project uses **hatchling** as the build backend:

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

## Package Manager

Use **uv** for all dependency management:

```bash
uv sync                     # Install project + core dependencies
uv sync --group dev         # Include dev dependencies (pytest, mkdocs, ruff, mypy, bandit)
uv sync --extra delta       # Include the optional Delta Lake extra
uv add <package>             # Add a new dependency
uv run <command>             # Run in the project's virtual environment
```

## pyproject.toml Structure

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "pyspark-excel-ds"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "pyspark>=3.5.0,<4.0.0",
    "pandas<3.0.0",
    "openpyxl>=3.1.0",
    "xlsxwriter>=3.2.0",
    "rich>=15.0.0",
]

[project.optional-dependencies]
delta = ["delta-spark>=3.2.0,<4.0.0"]

[dependency-groups]
dev = [
    "pytest>=8.0",
    "pytest-cov>=7.1.0",
    "mkdocs>=1.6.1",
    "mkdocs-material>=9.7.7",
    "ruff>=0.16.0",
    "mypy>=2.3.0",
    "bandit>=1.9.4",
    "pymarkdownlnt>=0.9.39",
]

[tool.pytest.ini_options]
minversion = "8.0"
addopts = "-ra -q"
testpaths = ["tests"]
pythonpath = ["src"]

[tool.hatch.build.targets.wheel]
packages = ["src/pys_excel"]
```

## Key Conventions

- **PySpark 3.5.x** is the target version — `pyspark>=3.5.0,<4.0.0`. This is
  intentionally kept below 4.0 (unlike some sibling projects in this repo) to
  keep `delta-spark` compatibility safe for the table-integration workflows.
- **Python ≥ 3.11**.
- **Delta Lake is optional** — declared under `[project.optional-dependencies]
  delta`, never under `[project.dependencies]`, so basic usage doesn't force
  network access to resolve Maven packages.
- Dev tooling (ruff, mypy, bandit, pymarkdownlnt, mkdocs, pytest) lives under
  `[dependency-groups] dev`.
- `pythonpath = ["src"]` ensures `pys_excel` is importable in tests without
  installing the package.
- Tool configuration (ruff, mypy, bandit, coverage) lives entirely in
  `pyproject.toml` — no separate `.flake8`/`mypy.ini`/`.bandit` files.

## Project Layout

```
pyspark-excel-ds/
├── src/pys_excel/    # Installable library package
├── examples/         # Standalone demo/tutorial scripts
├── tests/            # pytest test suite
├── docs/             # MkDocs documentation
├── scripts/          # generate_sample_data.py, run-all-examples.sh
├── pyproject.toml    # Single source of truth for config
├── Makefile          # install/test/lint/format/docs/build targets
└── mkdocs.yml        # Documentation site config
```

## Guidelines

- Do not use `requirements.txt` — all deps live in `pyproject.toml`.
- Do not switch away from the hatchling build backend.
- Use `uv` (not pip/poetry/conda) for all package operations in this project.
- Do not move `delta-spark` into `[project.dependencies]`.
- Pin the Spark major/minor ceiling (`pyspark>=3.5.0,<4.0.0`) — do not widen
  to `pyspark>=4.0.0` without re-validating Delta Lake compatibility.
- Use `make <target>` (see `Makefile`) for common workflows instead of
  memorizing raw `uv run` invocations.
