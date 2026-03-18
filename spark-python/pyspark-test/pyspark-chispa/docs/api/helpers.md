# String Helpers

Pure Python string helper utilities. These functions have **no PySpark
dependency** and can be unit-tested without a SparkSession.

## Source

```python title="src/data_frame/helper/string_helper.py"
--8<-- "src/data_frame/helper/string_helper.py"
```

## Functions

### dots_to_underscores

Replaces all dots with underscores. Designed for column name normalisation
when used with `modify_column_names`.

```python
from data_frame.helper.string_helper import dots_to_underscores

dots_to_underscores("first.name")           # "first_name"
dots_to_underscores("a.b.c.d")              # "a_b_c_d"
dots_to_underscores("no_dots_here")         # "no_dots_here"
```

### snake_case

Converts a string to `snake_case` by replacing spaces, hyphens, and dots with
underscores and lowercasing.

```python
from data_frame.helper.string_helper import snake_case

snake_case("Hello World")              # "hello_world"
snake_case("my-column-name")           # "my_column_name"
snake_case("My Column.Name-here")      # "my_column_name_here"
```

### truncate

Truncates a string to a maximum length, appending a suffix (default `"..."`)
when trimmed.

```python
from data_frame.helper.string_helper import truncate

truncate("hello", 10)                  # "hello"       (within limit)
truncate("hello world", 8)            # "hello..."    (truncated)
truncate("hello world", 8, suffix="~") # "hello w~"   (custom suffix)
```

!!! warning "Invalid max_length"
    Raises `ValueError` if `max_length` is less than the length of `suffix`.

## Run Tests

```bash
uv run pytest tests/transformation/ -v -k "DotsToUnderscores or SnakeCase or Truncate"
```
