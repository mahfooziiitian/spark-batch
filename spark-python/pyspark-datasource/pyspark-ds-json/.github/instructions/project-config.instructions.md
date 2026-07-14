---
applyTo: "pyproject.toml,requirements.txt"
---

# Project Configuration

## Build System

This project uses **setuptools** as the build backend, configured in `pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=42", "wheel"]
build-backend = "setuptools.build_meta"
```

Do not switch to Poetry, Hatch, or other build backends — keep setuptools.

## Dependencies — requirements.txt

Pin dependencies with compatible-release specifiers (`~=`) for reproducibility:

```
pyspark~=3.5.0
setuptools~=65.6.3
pytest
pandas
```

- `pyspark~=3.5.0` — allows `3.5.x` patch upgrades but not `3.6.0`.
- `setuptools~=65.6.3` — allows `65.6.x` patch upgrades.
- `pytest` and `pandas` — unpinned (latest compatible version).

When adding new dependencies, place them in `requirements.txt` (not in `pyproject.toml` `[project.dependencies]`). Use `~=` for version pinning when a specific range is needed.

## pytest Configuration

pytest is configured in `pyproject.toml` under `[tool.pytest.ini_options]`:

```toml
[tool.pytest.ini_options]
minversion = "6.0"
addopts = "-ra -q"
testpaths = ["tests"]
pythonpath = ["src"]
```

Key settings:

- **`pythonpath = ["src"]`** — adds `src/` to `sys.path` so imports like `from jsons.schema.class_schema.class_schema import ...` resolve correctly.
- **`testpaths = ["tests"]`** — pytest only discovers tests under `tests/`.
- **`addopts = "-ra -q"`** — shows a short summary of all non-passing tests with quiet output.
- **`minversion = "6.0"`** — enforces minimum pytest version.

## Guidelines

- Keep `pyproject.toml` minimal — only build-system and pytest config belong here.
- Do not add `[project]` metadata unless publishing the package to PyPI.
- Do not duplicate dependencies in both `requirements.txt` and `pyproject.toml`.
- Install dependencies with `pip install -r requirements.txt`.
