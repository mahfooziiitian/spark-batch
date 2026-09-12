# :material-null: NULL in Expressions

Spark 4.2 expressions are easiest to understand as either null-intolerant or null-aware.

______________________________________________________________________

## :material-sitemap: Overview

| Expression type     | Verified behavior                                      |
| ------------------- | ------------------------------------------------------ |
| Null-intolerant     | Returns `NULL` when a required input is `NULL`         |
| Null-aware helper   | Produces a useful non-NULL result for some NULL inputs |
| NaN-specific helper | Handles `NaN`, which is different from `NULL`          |

### :material-animation-play: Interactive Visualization — Expression Categories

<div id="viz-null-expression" class="ts-viz"></div>

Switch between representative expressions to see which ones propagate NULL and which ones intentionally handle it.

______________________________________________________________________

## :material-table: Verified Null-Intolerant Expressions

| Expression              | Result with NULL input |
| ----------------------- | ---------------------- |
| `CONCAT('John', NULL)`  | `NULL`                 |
| `CAST(NULL AS INT) + 5` | `NULL`                 |
| `UPPER(NULL)`           | `NULL`                 |
| `TO_DATE(NULL)`         | `NULL`                 |
| `LENGTH(NULL)`          | `NULL`                 |

These expressions propagate the unknown value instead of manufacturing a replacement.

______________________________________________________________________

## :material-table: Verified Null-Aware Expressions

| Expression                       | Result  |
| -------------------------------- | ------- |
| `COALESCE(NULL, 0)`              | `0`     |
| `IFNULL(NULL, 'N/A')`            | `'N/A'` |
| `NVL(NULL, -1)`                  | `-1`    |
| `NVL2(NULL, 'yes', 'no')`        | `'no'`  |
| `CONCAT_WS(',', 'a', NULL, 'b')` | `'a,b'` |

______________________________________________________________________

## :material-flask-outline: Verified Examples

### Null-intolerant expressions

```sql
SELECT
    CONCAT('John', NULL) AS concat_null,
    CAST(NULL AS INT) + 5 AS plus_null,
    UPPER(NULL) AS upper_null,
    TO_DATE(NULL) AS to_date_null,
    LENGTH(NULL) AS len_null;
```

```text
+-----------+---------+----------+------------+--------+
|concat_null|plus_null|upper_null|to_date_null|len_null|
+-----------+---------+----------+------------+--------+
|NULL       |NULL     |NULL      |NULL        |NULL    |
+-----------+---------+----------+------------+--------+
```

### Null-aware helpers

```sql
SELECT
    COALESCE(NULL, NULL, 7) AS coalesce_v,
    IFNULL(NULL, 'x') AS ifnull_v,
    NVL(NULL, 'y') AS nvl_v,
    NVL2(NULL, 'yes', 'no') AS nvl2_v,
    CONCAT_WS(',', 'a', NULL, 'b') AS concat_ws_v;
```

```text
+----------+--------+-----+------+-----------+
|coalesce_v|ifnull_v|nvl_v|nvl2_v|concat_ws_v|
+----------+--------+-----+------+-----------+
|7         |x       |y    |no    |a,b        |
+----------+--------+-----+------+-----------+
```

### `NaN` is not `NULL`

```sql
SELECT
    ISNAN(CAST('NaN' AS DOUBLE)) AS isnan_nan,
    ISNAN(NULL) AS isnan_null,
    NANVL(CAST('NaN' AS DOUBLE), 0.0D) AS nanvl_nan,
    NANVL(2.0D, 0.0D) AS nanvl_num;
```

```text
+---------+----------+---------+---------+
|isnan_nan|isnan_null|nanvl_nan|nanvl_num|
+---------+----------+---------+---------+
|true     |false     |0.0      |2.0      |
+---------+----------+---------+---------+
```

______________________________________________________________________

## :material-lightbulb-outline: Practical Takeaways

- Use null-intolerant expressions when NULL propagation is correct.
- Use `COALESCE`, `IFNULL`, `NVL`, `NVL2`, or `CONCAT_WS` when you need a fallback.
- Keep `NaN` handling separate from NULL handling.

<script src="../../assets/js/querying-nulls-viz.js"></script>
