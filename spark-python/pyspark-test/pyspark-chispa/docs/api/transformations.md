# Transformations

DataFrame-level transformation utilities. Each function receives a DataFrame
and returns a new DataFrame.

## Source

```python title="src/data_frame/transformation/df_transformations.py"
--8 < --"src/data_frame/transformation/df_transformations.py"
```

## Functions

### modify_column_names

Renames all columns using a transformation function. Useful for normalising
column names from external data sources.

```python
from data_frame.transformation.df_transformations import modify_column_names
from data_frame.helper.string_helper import dots_to_underscores

clean_df = modify_column_names(df, dots_to_underscores)
# "first.name" → "first_name"
# "person.favorite.number" → "person_favorite_number"
```

Works with any `str → str` function:

```python
upper_df = modify_column_names(df, str.upper)  # "name" → "NAME"
snake_df = modify_column_names(df, snake_case)  # "My Col" → "my_col"
```

### with_row_number

Adds a sequential row number column ordered by the specified column.

```python
from data_frame.transformation.df_transformations import with_row_number

numbered = with_row_number(df, order_col="created_at")
# Adds "row_number" column: 1, 2, 3, ...

# Custom alias
numbered = with_row_number(df, order_col="id", alias="rn")
```

### with_running_total

Adds a cumulative sum column over an ordered window, optionally partitioned.

```python
from data_frame.transformation.df_transformations import with_running_total

# Global running total
result = with_running_total(df, value_col="revenue", order_col="date")

# Partitioned running total (per region)
result = with_running_total(df, value_col="revenue", order_col="date", partition_col="region")
```

**Example:**

| step | val | running_total |
| --- | --- | --- |
| 1 | 10 | 10 |
| 2 | 20 | 30 |
| 3 | 30 | 60 |

### deduplicate

Removes duplicate rows, keeping either the first or last occurrence based on
an ordering column.

```python
from data_frame.transformation.df_transformations import deduplicate

# Keep earliest record per customer
deduped = deduplicate(df, subset=["customer_id"], order_col="created_at", keep="first")

# Keep latest record per customer
deduped = deduplicate(df, subset=["customer_id"], order_col="created_at", keep="last")
```

!!! warning "Invalid keep value"
    Raises `ValueError` if `keep` is not `"first"` or `"last"`.

### filter_nulls

Removes rows where any of the specified columns contain null values.

```python
from data_frame.transformation.df_transformations import filter_nulls

clean = filter_nulls(df, columns=["id", "name", "email"])
```

| id | name | email | kept? |
| --- | --- | --- | --- |
| 1 | Alice | alice@x.com | ✅ |
| null | Bob | bob@x.com | ❌ |
| 3 | null | carol@x.com | ❌ |
| 4 | Dave | null | ❌ |

## Run Tests

```bash
uv run pytest tests/transformation/ -v
```
