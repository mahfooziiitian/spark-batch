---
applyTo: "**/*.py"
---

# Python Conventions

## Style

- Follow **PEP 8**. Use 4-space indentation, `snake_case` for functions and
  variables, `PascalCase` for classes, and `UPPER_SNAKE_CASE` for constants.
- Keep lines ≤ 88 characters (Black default).
- Format with **Black** and order imports with **isort**; lint with **flake8**.

## Imports

- Use explicit imports. **Never** `from pyspark.sql.functions import *`.
- Prefer `from pyspark.sql import functions as F` (a lowercase `f` alias is
  acceptable if used consistently within a file).
- Group imports: standard library → third-party → first-party (`spark_xml`).

## Type Hints & Docstrings

- Add type hints to reusable functions in `src/spark_xml/`.
- Use **Google-style** docstrings (`Args:`, `Returns:`, `Raises:`) for library
  functions. Example/demo scripts may use a single-line module docstring.

## Environment & Paths

- Read the Spark master from `SPARK_MASTER` with a `local[*]` fallback:
  `os.environ.get("SPARK_MASTER", "local[*]")`.
- Read input/output paths from environment variables with `/tmp/...` fallbacks.

## Things to Avoid

- Do **not** use `print(df.schema)` — use `df.printSchema()`.
- Do **not** use `len(df.collect())` — use `df.count()`.
