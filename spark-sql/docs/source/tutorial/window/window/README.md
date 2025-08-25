# Window function

A window function lets you calculate values over a set of rows (a window), without reducing the number of rows in the result set.

*For each row, calculate something compared to nearby rows.*

## Syntax

```sql
<function>([expression]) OVER (
  [PARTITION BY ...]
  [ORDER BY ...]
  [ROWS or RANGE frame]
)
```

Clause |Purpose
---|---
PARTITION BY| Divides the dataset into independent groups
ORDER BY |Orders rows within each partition
ROWS / RANGE| Limits how many rows are used for the calculation
