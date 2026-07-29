# DataFrame Equality

DataFrame sorting and comparison utilities for validating data quality.

## Source

```python title="src/data_frame/equality/df_equality.py"
--8 < --"src/data_frame/equality/df_equality.py"
```

## Functions

### sort_columns

Reorders DataFrame columns alphabetically in ascending or descending order.

```python
from data_frame.equality.df_equality import sort_columns

sorted_df = sort_columns(df, "asc")  # columns A → Z
sorted_df = sort_columns(df, "desc")  # columns Z → A
```

!!! warning "Invalid sort order"
    Raises `ValueError` if `sort_order` is not `"asc"` or `"desc"`.

### columns_match

Checks whether two DataFrames have identical column names in the same order.
Returns a boolean — useful for pre-validation before joins or unions.

```python
from data_frame.equality.df_equality import columns_match

if columns_match(df1, df2):
    combined = df1.unionByName(df2)
```

### row_diff

Returns rows present in the left DataFrame but not in the right.
Uses Spark's `subtract` operation.

```python
from data_frame.equality.df_equality import row_diff

missing = row_diff(expected_df, actual_df)
print(f"{missing.count()} rows are missing from actual")
```

!!! note
    `subtract` compares entire rows. Column order and types must match.

### union_dedup

Unions two DataFrames by column name and removes duplicate rows.

```python
from data_frame.equality.df_equality import union_dedup

combined = union_dedup(batch_1, batch_2)
```

## Run Tests

```bash
uv run pytest tests/equality/ -v
```
