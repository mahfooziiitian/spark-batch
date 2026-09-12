# :material-table-pivot: `UNPIVOT`

`UNPIVOT` converts wide columns back into labelled rows. It is the cleanest Spark SQL way to normalize pivoted or spreadsheet-shaped data before downstream filtering and aggregation.

______________________________________________________________________

## :material-sitemap: Overview

```mermaid
graph LR
    A[Wide table] --> B[UNPIVOT selected columns]
    B --> C[Long table with label and value columns]
```

### :material-animation-play: Interactive Visualization — Wide-to-Long Expansion

<div id="viz-unpivot-rows" class="ts-viz"></div>

Switch between default mode and `INCLUDE NULLS` to see when Spark drops rows and when it keeps missing values.

______________________________________________________________________

## :material-pin: Syntax

```sql
SELECT unpivot_name_col, unpivot_value_col [, ...]
FROM source_table
UNPIVOT [INCLUDE NULLS] (
    value_col
    FOR name_col IN (source_col [AS alias], ...)
);
```

Aliases after `AS` are identifiers such as `Q1`, not quoted string literals such as `'Q1'`.

______________________________________________________________________

## :material-magnify: Verified behavior

1. Default `UNPIVOT` drops rows whose generated value column is `NULL`.
2. `UNPIVOT INCLUDE NULLS` keeps those rows.
3. If you omit aliases, Spark uses the source column names such as `q1`, `q2`, `q3`.
4. Multi-value unpivot works in Spark 4.2: `((qty, rev) FOR quarter IN ((q1_qty, q1_rev) AS Q1, ...))`.
5. The generated label column is a string output, even when aliases are written as bare identifiers.

______________________________________________________________________

## :material-flask-outline: Practical examples

### Setup

```sql
CREATE OR REPLACE TEMP VIEW quarterly_revenue AS
SELECT * FROM VALUES
    ('East',  2024, 100.0, 200.0, 150.0, 300.0),
    ('West',  2024, 250.0, 180.0, 320.0, 270.0),
    ('North', 2024,  90.0,  NULL, 110.0, 140.0)
AS quarterly_revenue(region, yr, q1, q2, q3, q4);
```

### Default mode excludes `NULL` outputs

```sql
SELECT region, yr, quarter, revenue
FROM quarterly_revenue
UNPIVOT (
    revenue
    FOR quarter IN (q1 AS Q1, q2 AS Q2, q3 AS Q3, q4 AS Q4)
)
ORDER BY region, quarter;
```

Verified result: `North / Q2` is absent because `q2` was `NULL`.

### `INCLUDE NULLS` keeps them

```sql
SELECT region, yr, quarter, revenue
FROM quarterly_revenue
UNPIVOT INCLUDE NULLS (
    revenue
    FOR quarter IN (q1 AS Q1, q2 AS Q2, q3 AS Q3, q4 AS Q4)
)
ORDER BY region, quarter;
```

Verified result: `North / Q2 / NULL` appears as a normal output row.

### Omitting aliases uses source column names

```sql
SELECT region, yr, quarter, revenue
FROM quarterly_revenue
UNPIVOT (
    revenue
    FOR quarter IN (q1, q2, q3, q4)
)
ORDER BY region, quarter;
```

PySpark 4.2 returned `quarter` values `q1`, `q2`, `q3`, `q4` in this form.

### Multi-value unpivot

```sql
SELECT *
FROM (
    SELECT * FROM VALUES
        (2024, 10, 100, 20, NULL),
        (2025, 30, 300, NULL, 400)
    AS t(yr, q1_qty, q1_rev, q2_qty, q2_rev)
) src
UNPIVOT INCLUDE NULLS (
    (qty, rev)
    FOR quarter IN ((q1_qty, q1_rev) AS Q1, (q2_qty, q2_rev) AS Q2)
)
ORDER BY yr, quarter;
```

______________________________________________________________________

## :material-brain: When to use

| Scenario                                                 | Recommended pattern     |
| -------------------------------------------------------- | ----------------------- |
| Normalize wide columns into rows                         | `UNPIVOT`               |
| Preserve missing cells explicitly                        | `UNPIVOT INCLUDE NULLS` |
| Turn parallel metric columns into one label/value layout | Multi-value `UNPIVOT`   |
| Need a fallback on older engines                         | `STACK` or `UNION ALL`  |
