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

[tool.hatch.build.targets.wheel]
packages = ["src/custom_ds"]
```

## Package Manager

Use **uv** for all dependency management — never `pip install` directly:

```bash
uv sync              # Install project + dependencies into .venv
uv sync --group dev  # Include dev dependencies (pytest, mkdocs)
uv add <package>     # Add a new runtime dependency
uv run <command>     # Run any command inside the project's virtual environment
```

## Key Conventions

- **PySpark 4.x** is the target version — declare `pyspark>=4.0.0`.
- **PyArrow is required** — the Python Data Source API (`pyspark.sql.datasource`) uses Arrow to
  move batches between the JVM and Python workers. Always declare `pyarrow>=14.0.0` alongside
  `pyspark` or batch/streaming reads and writes will fail at runtime with
  `ModuleNotFoundError: No module named 'pyarrow'`.
- **Python ≥ 3.11**, **Java 17 (LTS)** — required runtime for Spark 4.
- All dependencies live in `pyproject.toml` — do not add a `requirements.txt`.
- Dev-only tools (pytest, mkdocs) go under `[dependency-groups] dev`.
- `pythonpath = ["src"]` in `[tool.pytest.ini_options]` makes `custom_ds` importable in tests
  without an editable install.

## Project Layout

```
pyspark-ds-custom/
├── src/custom_ds/    # Installable library: DataSource/Reader/Writer implementations
├── examples/         # Standalone runnable demo scripts
├── tests/            # pytest test suite
├── pyproject.toml    # Single source of truth for config
└── README.md
```

## Guidelines

- Do not use `requirements.txt` — all deps live in `pyproject.toml`.
- Do not switch away from the `hatchling` build backend.
- Use `uv` (not pip/poetry/conda) for all package operations in this project.
- Pin the major version for critical deps (`pyspark>=4.0.0`, `pyarrow>=14.0.0`); leave dev tools
  loosely pinned.
- After adding or upgrading a dependency, run `uv sync` and re-run the test suite before committing.
