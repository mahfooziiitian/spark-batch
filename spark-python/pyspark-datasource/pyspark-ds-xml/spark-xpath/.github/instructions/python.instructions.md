---
applyTo: "**/*.py"
---

# Python Conventions

## Version & Runtime

- Target **Python ≥ 3.11**. Use modern syntax: `match` statements, `str | None`
  union types, `tomllib`, etc.
- The project uses **[uv](https://docs.astral.sh/uv/)** for dependency
  management. Never suggest `pip install` in CI — use `uv sync` / `uv run`.

## Style

- Follow **PEP 8** formatting.
- Use **type hints** on all function signatures and return types:
  ```python
  def extract_field(xml: str, xpath_expr: str) -> str: ...
  ```
- Prefer `snake_case` for variables, functions, and modules.
- Use `PascalCase` for classes only.
- Keep line length ≤ **120 characters**.

## Imports

- Use **explicit imports** — never `from module import *`.
- Group imports in standard order: stdlib → third-party → local, separated by
  blank lines.
- Prefer importing specific names over entire modules:
  ```python
  # ✅ Good
  from pyspark.sql import SparkSession
  from pyspark.sql.types import StringType

  # ❌ Avoid
  from pyspark.sql.functions import *
  ```

## Strings & Formatting

- Use **f-strings** for interpolation (not `%` or `.format()`).
- Use **double quotes** for strings by default.
- Use triple-quoted strings for multi-line SQL or XML literals.

## Error Handling

- Catch specific exceptions, not bare `except:`.
- Add context to raised exceptions:
  ```python
  raise ValueError(f"Invalid XPath expression: {expr}") from e
  ```

## Docstrings — Google Style

Always use **[Google-style](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings)** docstrings. Every public module, class, function, and method must have one.

### Functions / Methods

```python
def extract_fields(xml: str, xpath_exprs: list[str], *, strip: bool = True) -> dict[str, str]:
    """Extract multiple values from XML using XPath expressions.

    Iterates over the given XPath expressions and returns a mapping of
    expression to extracted text value. Returns an empty string for any
    expression that does not match.

    Args:
        xml: The XML document as a string.
        xpath_exprs: XPath expressions to evaluate against the XML.
        strip: If True, strip leading/trailing whitespace from results.
            Defaults to True.

    Returns:
        A dict mapping each XPath expression to its extracted text value.

    Raises:
        ValueError: If any XPath expression is syntactically invalid.
        TypeError: If ``xml`` is not a string.

    Example:
        >>> extract_fields("<r><a>1</a></r>", ["r/a"])
        {'r/a': '1'}
    """
```

### Classes

```python
class XPathProcessor:
    """Processes XML data using PySpark XPath functions.

    Wraps a SparkSession and provides convenience methods for registering
    XML DataFrames and running XPath queries.

    Attributes:
        spark: The active SparkSession instance.
        view_name: Name of the registered temp view.
    """

    def __init__(self, spark: SparkSession, view_name: str = "xml_data") -> None:
        """Initialise the processor.

        Args:
            spark: An active SparkSession.
            view_name: Temp view name to register DataFrames under.
                Defaults to ``"xml_data"``.
        """
```

### Modules

```python
"""Basic XML data parsing with PySpark xpath_string.

This module demonstrates loading inline XML strings into a DataFrame
and extracting header fields using Spark SQL XPath functions.

Typical usage:
    $ uv run python examples/xml_data_parsing.py
"""
```

### Short One-Liners (only for trivially obvious functions)

```python
def stop(self) -> None:
    """Stop the SparkSession."""
```

### Key Rules

- **First line**: imperative summary ending with a period (`"""Do X."""`).
- **Blank line** between summary and body sections.
- **Args**: one entry per parameter, type info lives in the signature (not
  repeated in the docstring). Continuation lines are indented 4 extra spaces.
- **Returns**: describe the return value and its type semantics.
- **Raises**: list every exception the function explicitly raises.
- **Example** (optional): include a `>>>` doctest for non-trivial logic.
- Do **not** use reST (`:param:`), NumPy, or Epydoc styles.

## Modules & Packages

- Every package directory must have an `__init__.py` (can be empty).
- Use `if __name__ == "__main__":` guards for runnable scripts.
