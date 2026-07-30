# Joins

All join types with JSON DataFrames: inner, left, right, full, semi, anti, and broadcast.

## Usage

```python title="examples/03_dataframe/06_joins.py"
--8<-- "examples/03_dataframe/06_joins.py"
```

## Join Types

| Type | Keeps |
|------|-------|
| `inner` | Matching rows from both sides |
| `left` / `left_outer` | All left rows + matching right |
| `right` / `right_outer` | All right rows + matching left |
| `full` / `full_outer` | All rows from both sides |
| `left_semi` | Left rows with a match (no right columns) |
| `left_anti` | Left rows without a match |
| `cross` | Cartesian product (use carefully!) |

!!! tip "Broadcast Joins"
    For small lookup tables, use `F.broadcast(small_df)` to avoid shuffle:
    ```python
    df.join(F.broadcast(lookup_df), "key")
    ```

## Run

```bash
python examples/03_dataframe/06_joins.py
```
