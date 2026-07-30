---
applyTo: "{pyproject.toml,requirements.txt}"
---

# Project Configuration

## Build System

This project uses **hatchling** as the build backend, configured in `pyproject.toml`:

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

## Package Manager

Use **uv** for dependency management:

```bash
uv sync              # Install project + dependencies
uv sync --group dev  # Include dev dependencies (pytest, mkdocs)
uv add <package>     # Add a new dependency
uv run <command>     # Run in the project's virtual environment
```

## pyproject.toml Structure

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "pyspark-ds-json"
version = "0.1.0"
description = "PySpark 4 JSON Datasource — tutorials, demos, and reusable library"
requires-python = ">=3.11"
dependencies = [
    "pyspark>=4.0.0",
    "pandas",
]

[dependency-groups]
dev = [
    "pytest>=6.0",
    "mkdocs>=1.6.1",
    "mkdocs-material>=9.7.7",
]

[tool.pytest.ini_options]
minversion = "6.0"
addopts = "-ra -q"
testpaths = ["tests"]
pythonpath = ["src"]

[tool.hatch.build.targets.wheel]
packages = ["src/pys_json"]
```

## Key Conventions

- **PySpark 4.x** is the target version — use `pyspark>=4.0.0` in dependencies.
- **Python ≥ 3.11** — required for PySpark 4 and modern syntax.
- **Java 17** (LTS) — required runtime for Spark 4.
- Dependencies are declared in `pyproject.toml` `[project.dependencies]` — no separate `requirements.txt` needed.
- Dev dependencies go in `[dependency-groups] dev`.
- `pythonpath = ["src"]` ensures `pys_json` is importable in tests.

## Project Layout

```
pyspark-ds-json/
├── src/pys_json/     # Installable library package
├── examples/         # Standalone demo/tutorial scripts
├── tests/            # pytest test suite
├── docs/             # MkDocs documentation
├── pyproject.toml    # Single source of truth for config
└── mkdocs.yml        # Documentation site config
```

## Guidelines

- Do not use `requirements.txt` — all deps live in `pyproject.toml`.
- Do not switch away from hatchling build backend.
- Use `uv` (not pip) for all package operations.
- Keep `pyproject.toml` as the single source of truth for project metadata, dependencies, and tool config.
- Pin major versions for critical deps (`pyspark>=4.0.0`), leave dev tools loosely pinned.
