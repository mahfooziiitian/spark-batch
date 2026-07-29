# Column Functions

Column-level transformation functions. Each receives a PySpark `Column` and
returns a transformed `Column`, making them composable inside `withColumn` calls.

## Source

```python title="src/data_frame/columns/column_equality.py"
--8 < --"src/data_frame/columns/column_equality.py"
```

## Functions

### remove_non_word_characters

Strips everything except word characters (`\w`) and whitespace (`\s`).

```python
from data_frame.columns.column_equality import remove_non_word_characters

df = df.withColumn("clean_name", remove_non_word_characters(F.col("name")))
```

| Input | Output |
| --- | --- |
| `"jo&&se"` | `"jose"` |
| `"**li**"` | `"li"` |
| `"matt7"` | `"matt7"` (digits are word chars) |
| `None` | `None` |

### normalize_whitespace

Collapses consecutive whitespace (spaces, tabs, newlines) into a single space
and trims leading/trailing whitespace.

```python
from data_frame.columns.column_equality import normalize_whitespace

df = df.withColumn("clean", normalize_whitespace(F.col("text")))
```

| Input | Output |
| --- | --- |
| `"hello   world"` | `"hello world"` |
| `"  padded  "` | `"padded"` |
| `"a\t\tb"` | `"a b"` |

### extract_email_domain

Extracts the domain portion (after `@`) from an email address column.
Returns `null` if the input contains no `@` sign.

```python
from data_frame.columns.column_equality import extract_email_domain

df = df.withColumn("domain", extract_email_domain(F.col("email")))
```

| Input | Output |
| --- | --- |
| `"alice@example.com"` | `"example.com"` |
| `"invalid-email"` | `null` |
| `None` | `null` |

### title_case

Capitalises the first letter of each word using Spark's `initcap`.

```python
from data_frame.columns.column_equality import title_case

df = df.withColumn("title", title_case(F.col("text")))
```

### null_safe_trim

Trims whitespace while explicitly preserving `null` values (does not convert
`null` to empty string).

```python
from data_frame.columns.column_equality import null_safe_trim

df = df.withColumn("trimmed", null_safe_trim(F.col("text")))
```

## Run Tests

```bash
uv run pytest tests/columns/ -v
```
