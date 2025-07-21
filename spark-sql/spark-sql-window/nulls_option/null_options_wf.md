# nulls_option

Specifies whether to skip null values when evaluating the window function.

`RESPECT NULLS` means not skipping null values, while `IGNORE NULLS` means skipping.

If not specified, the `default` is `RESPECT NULLS`.

## Syntax

```sql

{ IGNORE | RESPECT } NULLS
```

Note: `Only LAG | LEAD | NTH_VALUE | FIRST_VALUE | LAST_VALUE can be used with IGNORE NULLS`.

## 🎯 1. Ordering with NULLs in Window Functions

By default, Spark SQL follows ANSI SQL behavior:

1. ORDER BY col ASC → NULLs come last
2. ORDER BY col DESC → NULLs come first

You can override this using:

```sql

ORDER BY col ASC NULLS FIRST
ORDER BY col DESC NULLS LAST
```

```sql

SELECT *,
  ROW_NUMBER() OVER (ORDER BY amount ASC NULLS FIRST) AS rn_null_first,
  ROW_NUMBER() OVER (ORDER BY amount ASC NULLS LAST) AS rn_null_last
FROM (
  SELECT * FROM VALUES
    ('Alice', 100),
    ('Bob', NULL),
    ('Carol', 200)
  AS data(name, amount)
);
```
