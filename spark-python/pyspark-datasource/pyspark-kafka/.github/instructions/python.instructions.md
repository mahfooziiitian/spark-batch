---
applyTo: "**/*.py"
---

# Python Coding Style

## General

- Target **Python 3.11+**; use modern syntax (type hints, `match` statements, f-strings).
- Follow **PEP 8** naming: `snake_case` for functions/variables, `PascalCase` for classes, `UPPER_SNAKE_CASE` for constants.
- Use **type hints** on function signatures; prefer `str`, `int`, `list[str]`, `dict[str, Any]` over `typing` equivalents where possible.
- Keep functions short and focused — one clear responsibility per function.
- Prefer early returns over deeply nested `if` blocks.

## Imports

- Group imports: standard library → third-party → local, separated by blank lines.
- Use explicit imports (`from pyspark.sql import SparkSession`) rather than wildcard imports.
- Place `os.environ` assignments (e.g., `PYSPARK_PYTHON`) at module level, before the `if __name__` block.

## Documentation

- Add docstrings to public functions and classes using triple double-quotes.
- Use inline comments sparingly — only when the "why" is not obvious from the code.

## Error Handling

- Catch specific exceptions; avoid bare `except:`.
- Prefer logging over `print()` for non-trivial scripts.

## Project Conventions

- Entry-point scripts use `if __name__ == '__main__':` guards.
- Configuration is read via `ConfigReader` (wrapping `configparser`) for JDBC and external settings.
- Environment variables (`DATA_HOME`, `PYSPARK_PYTHON`) are expected to be set externally.
