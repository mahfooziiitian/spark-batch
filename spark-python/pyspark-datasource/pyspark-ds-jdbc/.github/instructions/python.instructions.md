---
applyTo: "**/*.py"
---

# Python Coding Standards

## Language Version

- Target **Python ≥ 3.11**; use modern syntax (match/case, `type` aliases, `ExceptionGroup`, etc.) where appropriate.

## Style & Formatting

- Follow **PEP 8** with a maximum line length of **120 characters**.
- Use **4 spaces** for indentation (no tabs).
- Use **snake_case** for functions, variables, and module names.
- Use **PascalCase** for class names.
- Use **UPPER_SNAKE_CASE** for module-level constants.

## Imports

- Order imports in three groups separated by blank lines:
  1. Standard library (`os`, `configparser`, `typing`)
  2. Third-party packages (`pyspark`, `pytest`)
  3. Local project modules (`utils.spark_util`, `utils.config_reader`)
- Prefer explicit imports over wildcard imports (`from module import name`, not `from module import *`).
- Use `from __future__ import annotations` only if needed for forward references.

## Type Hints

- Add type hints to function signatures (parameters and return types).
- Use built-in generics (`dict`, `list`, `tuple`) instead of `typing.Dict`, `typing.List`, `typing.Tuple` (Python 3.11+).
- Use `str | None` union syntax instead of `Optional[str]`.

## Docstrings

- Use triple double-quoted docstrings (`"""..."""`) for modules, classes, and public functions.
- Follow **Google style** docstring format:
  ```python
  def function(param: str) -> bool:
      """Short summary.

      Args:
          param: Description of param.

      Returns:
          Description of return value.
      """
  ```

## Error Handling

- Catch specific exceptions rather than bare `except:` or `except Exception:`.
- Provide context in error messages referencing the operation that failed.
- Use `raise ... from err` to preserve exception chains.

## General Practices

- Prefer f-strings for string formatting.
- Use `pathlib.Path` for file path manipulation where practical.
- Use context managers (`with` statements) for resource management (files, Spark sessions in tests).
- Keep functions focused — each function should do one thing.
- Avoid mutable default arguments (`def f(items=[])`); use `None` and assign inside the function body.
