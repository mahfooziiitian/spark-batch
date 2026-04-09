---
applyTo: "pyproject.toml,poetry.lock"
---

# Poetry Project Configuration

## Package Manager

- This project uses **Poetry** for dependency management and packaging.
- Build backend: **poetry-core** (`poetry.core.masonry.api`).

## pyproject.toml Structure

```toml
[tool.poetry]
name = "pyspark-ds-jdbc"
version = "0.1.0"
packages = [{include = "*", from = "src"}]

[tool.poetry.dependencies]
python = "^3.11"
pyspark = "^3.5.1"

[tool.poetry.group.dev.dependencies]
pytest = "^8.2.2"

[tool.pytest.ini_options]
minversion = "6.0"
addopts = "-ra -q"
pythonpath = ["src"]
testpaths = ["tests", "integration"]

[build-system]
requires = ["poetry-core"]
build-backend = "poetry.core.masonry.api"
```

### Key Sections

- **`[tool.poetry]`** — Project metadata; `packages` maps `src/` as the source root.
- **`[tool.poetry.dependencies]`** — Runtime dependencies; Python ^3.11 and PySpark ^3.5.1.
- **`[tool.poetry.group.dev.dependencies]`** — Dev-only dependencies (pytest).
- **`[tool.pytest.ini_options]`** — pytest configuration:
  - `pythonpath = ["src"]` — makes `src/` importable without installing.
  - `testpaths = ["tests", "integration"]` — directories pytest scans for tests.
  - `addopts = "-ra -q"` — show summary of all non-passing tests, quiet output.
- **`[build-system]`** — Uses `poetry-core` as the PEP 517 build backend.

## Common Poetry Commands

```bash
# Install all dependencies (including dev)
poetry install

# Add a runtime dependency
poetry add <package>

# Add a dev dependency
poetry add --group dev <package>

# Update dependencies to latest compatible versions
poetry update

# Run a command inside the virtual environment
poetry run <command>

# Run tests
poetry run pytest

# Show installed packages
poetry show

# Build the package
poetry build

# Lock dependencies without installing
poetry lock
```

## Guidelines

- Keep `poetry.lock` committed to version control for reproducible builds.
- Use caret (`^`) version constraints for flexibility with patch/minor updates.
- Add new test utilities or fixtures as dev dependencies only (`--group dev`).
- JDBC driver JARs are loaded at runtime via `spark.jars.packages` — they are **not** Python dependencies and should not be added to `pyproject.toml`.
- When adding a new source module, ensure it is under `src/` so it is automatically included by `packages = [{include = "*", from = "src"}]`.
