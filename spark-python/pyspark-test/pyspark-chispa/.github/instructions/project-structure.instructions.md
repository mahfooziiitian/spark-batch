---
applyTo: "{**/pyproject.toml,**/requirements.txt,**/uv.lock}"
---

# Project Structure & Configuration Instructions

## pyproject.toml Is the Single Source of Truth

All configuration lives in `pyproject.toml`:
- **Project metadata** — `[project]`
- **Dependencies** — `[project.dependencies]` and `[dependency-groups]`
- **pytest** — `[tool.pytest.ini_options]`
- **ruff** — `[tool.ruff]` and `[tool.ruff.lint]`
- **mypy** — `[tool.mypy]`
- **taskipy** — `[tool.taskipy.tasks]`

Do not create standalone config files (`.flake8`, `mypy.ini`, `setup.cfg`, etc.).

## Dependency Management

Use **uv** for all dependency operations:

```bash
uv add <package>              # add a runtime dependency
uv add --group dev <package>  # add a dev dependency
uv sync --group dev           # install all deps including dev
uv lock                       # regenerate uv.lock
```

### Dependency groups

| Group     | Contents                                               |
| --------- | ------------------------------------------------------ |
| runtime   | `pyspark`, `pandas`, `numpy`                           |
| dev       | `chispa`, `pytest-*`, `ruff`, `mypy`, `mkdocs-*`, `taskipy` |

## Source & Test Layout

```
src/                 ← pythonpath (configured in pytest)
  data_frame/
    <domain>/
      __init__.py
      <module>.py

tests/               ← testpaths (configured in pytest)
  conftest.py        ← shared fixtures (SparkSession)
  <domain>/
    test_<module>.py
```

**Rules:**
- Test directory mirrors source directory 1:1.
- One test file per source module.
- Shared fixtures in `tests/conftest.py` only — never in individual test files.
- `__init__.py` files in all source packages (can be empty).
- Test directories do NOT need `__init__.py` (pytest discovers automatically).

## Adding a New Module

1. Create source file: `src/data_frame/<domain>/<module>.py`
2. Ensure `src/data_frame/<domain>/__init__.py` exists.
3. Create test file: `tests/<domain>/test_<module>.py`
4. Import and use the shared `spark` fixture from `conftest.py`.
5. Organise tests into a class: `class Test<FunctionName>:`.
6. Run `uv run task check` to validate.

## CI Checklist (task check)

The `check` task runs the full pipeline in order:
1. `ruff check src tests` — lint
2. `ruff format --check src tests` — format verification
3. `mypy src` — type checking
4. `pytest -x --tb=short` — tests

All four must pass before committing.
