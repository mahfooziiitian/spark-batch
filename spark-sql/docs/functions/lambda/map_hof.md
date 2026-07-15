# :material-code-json: Map Higher-Order Functions

Map HOFs let you filter, transform, and merge `MAP` columns using lambda expressions
that receive both the **key** and the **value** of each map entry.

---

## :material-table: Quick Reference

| HOF | Lambda signature | Returns | Description |
|-----|-----------------|---------|-------------|
| `MAP_FILTER(map, (k,v) -> bool)` | `(k, v) -> bool` | Map | Keep entries where lambda is true |
| `TRANSFORM_VALUES(map, (k,v) -> v')` | `(k, v) -> new_val` | Map | Replace each value |
| `TRANSFORM_KEYS(map, (k,v) -> k')` | `(k, v) -> new_key` | Map | Replace each key |
| `MAP_ZIP_WITH(m1, m2, (k,v1,v2) -> v')` | `(k, v1, v2) -> new_val` | Map | Merge two maps by shared key |

---

## :material-filter: MAP_FILTER

```sql
-- Keep only entries with value > 1
SELECT MAP_FILTER(MAP('a', 1, 'b', 2, 'c', 3), (k, v) -> v > 1) AS filtered;
-- {b -> 2, c -> 3}

-- Keep entries where key starts with 'err'
SELECT MAP_FILTER(
    MAP('err_login', 12, 'info_page', 150, 'err_db', 5, 'warn_slow', 8),
    (k, v) -> k LIKE 'err%'
) AS error_counts;
-- {err_login -> 12, err_db -> 5}

-- Real table: keep only high-priority attributes
SELECT
    order_id,
    MAP_FILTER(attributes, (k, v) -> k IN ('priority', 'region')) AS key_attrs
FROM orders;

-- Use as a row predicate: order has a 'promo' key with value 'true'
SELECT order_id
FROM orders
WHERE size(MAP_FILTER(attributes, (k, v) -> k = 'promo' AND v = 'true')) > 0;
```

---

## :material-transform: TRANSFORM_VALUES

```sql
-- Uppercase all map values
SELECT TRANSFORM_VALUES(
    MAP('city', 'london', 'country', 'uk'),
    (k, v) -> UPPER(v)
) AS upper_map;
-- {city -> LONDON, country -> UK}

-- Apply different transformation per key
SELECT TRANSFORM_VALUES(
    MAP('price', 100, 'tax', 8, 'shipping', 5),
    (k, v) -> CASE
        WHEN k = 'price' THEN v
        ELSE CAST(ROUND(v * 1.1, 0) AS INT)
    END
) AS adjusted;
-- {price -> 100, tax -> 9, shipping -> 6}

-- Normalise numeric scores to percentage of max
SELECT
    product_id,
    TRANSFORM_VALUES(
        metric_scores,
        (k, v) -> ROUND(v * 100.0 / MAP_VALUES_MAX, 1)
    ) AS pct_scores
FROM product_metrics;

-- Add a prefix to all values
SELECT TRANSFORM_VALUES(
    MAP('status', 'active', 'role', 'admin'),
    (k, v) -> CONCAT('val:', v)
) AS prefixed;
-- {status -> val:active, role -> val:admin}
```

---

## :material-rename-box: TRANSFORM_KEYS

```sql
-- Uppercase all keys
SELECT TRANSFORM_KEYS(
    MAP('name', 'Alice', 'age', '30'),
    (k, v) -> UPPER(k)
) AS upper_keys;
-- {NAME -> Alice, AGE -> 30}

-- Add a namespace prefix to keys
SELECT TRANSFORM_KEYS(
    MAP('id', 1, 'score', 95),
    (k, v) -> CONCAT('user.', k)
) AS namespaced;
-- {user.id -> 1, user.score -> 95}

-- Real table: prefix keys with column category
SELECT
    event_id,
    TRANSFORM_KEYS(metadata, (k, v) -> CONCAT(event_type, '_', k)) AS prefixed_meta
FROM events;
```

!!! warning "Duplicate keys after TRANSFORM_KEYS"
    If the lambda maps two different keys to the same new key, the behaviour
    is undefined (last value wins or error depending on Spark version).
    Ensure the transformation preserves key uniqueness.

---

## :material-merge: MAP_ZIP_WITH

```sql
-- Add values from two maps by shared key
SELECT MAP_ZIP_WITH(
    MAP('q1', 100, 'q2', 150),
    MAP('q1', 120, 'q2', 130),
    (k, v1, v2) -> v1 + v2
) AS combined;
-- {q1 -> 220, q2 -> 280}

-- Handle keys missing in one map with COALESCE
SELECT MAP_ZIP_WITH(
    MAP('a', 10, 'b', 20),
    MAP('a', 5),                     -- 'b' missing
    (k, v1, v2) -> COALESCE(v1, 0) + COALESCE(v2, 0)
) AS safe_merged;
-- {a -> 15, b -> 20}

-- Delta between two snapshots
SELECT MAP_ZIP_WITH(
    last_week_counts,
    this_week_counts,
    (k, prev, curr) -> curr - prev
) AS weekly_delta
FROM weekly_metrics;

-- Take max value per key across two maps
SELECT MAP_ZIP_WITH(
    forecast_map,
    actuals_map,
    (k, f, a) -> GREATEST(COALESCE(f, 0), COALESCE(a, 0))
) AS upper_bound
FROM projections;
```

---

## :material-compare: Map HOF vs SQL Alternatives

| Need | Map HOF | SQL Alternative |
|------|---------|----------------|
| Filter map entries by key | `MAP_FILTER((k,v) -> k = ...)` | `element_at` + `map_contains_key` (single key only) |
| Filter map entries by value | `MAP_FILTER((k,v) -> v > ...)` | No direct alternative |
| Replace all values | `TRANSFORM_VALUES` | No direct alternative |
| Rename all keys | `TRANSFORM_KEYS` | No direct alternative |
| Merge two maps (all keys) | `map_concat` | `map_concat` (last wins, no custom merge) |
| Merge two maps (custom per-key) | `MAP_ZIP_WITH` | No direct alternative |

---

## :material-alert-circle: Common Mistakes

| Mistake | Behaviour | Fix |
|---------|-----------|-----|
| Using `TRANSFORM_KEYS` creating duplicate keys | Undefined / last-wins | Verify key uniqueness before transforming |
| `MAP_ZIP_WITH` on maps with disjoint keys | Only shared keys appear in result | Use `COALESCE(v1, default)` for missing-key handling |
| Treating `MAP_FILTER` result as boolean | Returns map, not bool | Use `size(MAP_FILTER(...)) > 0` in `WHERE` |
| Forgetting `CAST` when transforming numeric string values | Type mismatch | `CAST(element_at(map, key) AS INT)` before arithmetic |
