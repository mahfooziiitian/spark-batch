# :material-map: Map Functions

Comprehensive reference for Spark SQL functions that **create, query, and transform maps**
(key-value collections).

### :material-sitemap: Overview

```mermaid
graph LR
    A[Keys and Values] --> B[Map Functions]
    B --> C[Map Type]
```

## 📌 Creating Maps

### MAP — Build from Key-Value Pairs

```sql
SELECT MAP(1, 'a', 2, 'b', 3, 'c');
-- Result: {1 -> a, 2 -> b, 3 -> c}

SELECT MAP('name', 'Alice', 'city', 'NYC');
-- Result: {name -> Alice, city -> NYC}
```

### MAP_FROM_ARRAYS — Build from Two Arrays

```sql
SELECT MAP_FROM_ARRAYS(ARRAY(1, 2, 3), ARRAY('a', 'b', 'c'));
-- Result: {1 -> a, 2 -> b, 3 -> c}
```

> All elements in the keys array must be non-NULL.

### MAP_FROM_ENTRIES — Build from Array of Structs

```sql
SELECT MAP_FROM_ENTRIES(ARRAY(STRUCT(1, 'a'), STRUCT(2, 'b')));
-- Result: {1 -> a, 2 -> b}
```

---

## 📌 Querying Maps

### ELEMENT_AT — Get Value by Key

```sql
SELECT ELEMENT_AT(MAP(1, 'a', 2, 'b'), 2);
-- Result: b
```

### MAP_CONTAINS_KEY — Check Key Existence

```sql
SELECT MAP_CONTAINS_KEY(MAP(1, 'a', 2, 'b'), 1);
-- Result: true

SELECT MAP_CONTAINS_KEY(MAP(1, 'a', 2, 'b'), 3);
-- Result: false
```

### MAP_KEYS — Extract Keys as Array

```sql
SELECT MAP_KEYS(MAP(1, 'a', 2, 'b'));
-- Result: [1, 2]
```

### MAP_VALUES — Extract Values as Array

```sql
SELECT MAP_VALUES(MAP(1, 'a', 2, 'b'));
-- Result: [a, b]
```

### MAP_ENTRIES — Extract as Array of Structs

```sql
SELECT MAP_ENTRIES(MAP(1, 'a', 2, 'b'));
-- Result: [{1, a}, {2, b}]
```

---

## 📌 Merging Maps

### MAP_CONCAT — Simple Merge (Later Wins)

```sql
SELECT MAP_CONCAT(MAP(1, 'a', 2, 'b'), MAP(2, 'x', 3, 'c'));
-- Result: {1 -> a, 2 -> x, 3 -> c}  (key 2 overwritten)
```

### MAP_ZIP_WITH — Merge with Per-Key Lambda

```sql
MAP_ZIP_WITH(map1, map2, (key, val1, val2) -> expression)
```

For each key in the **union** of both maps, calls the lambda with the key and both values
(NULL if the key is absent in one map).

#### Sum Counts per Key

```sql
WITH sample AS (
  SELECT MAP('a', 1, 'b', 2) AS m1, MAP('b', 20, 'c', 30) AS m2
)
SELECT MAP_ZIP_WITH(m1, m2, (k, v1, v2) -> COALESCE(v1, 0) + COALESCE(v2, 0)) AS merged
FROM sample;
-- Result: {a -> 1, b -> 22, c -> 30}
```

#### Overlay / Fallback (Prefer Right)

```sql
SELECT MAP_ZIP_WITH(m1, m2, (k, v1, v2) -> COALESCE(v2, v1)) AS overlaid
FROM sample;
-- Result: {a -> 1, b -> 20, c -> 30}
```

#### Compute Deltas

```sql
SELECT MAP_ZIP_WITH(m1, m2, (k, v1, v2) -> COALESCE(v2, 0) - COALESCE(v1, 0)) AS delta
FROM sample;
-- Result: {a -> -1, b -> 18, c -> 30}
```

#### Build Diff Structs per Key

```sql
SELECT MAP_ZIP_WITH(
  m1, m2,
  (k, v1, v2) -> NAMED_STRUCT('old', v1, 'new', v2, 'changed', v1 <> v2)
) AS diff_map
FROM sample;
```

#### Filter After Zipping

```sql
WITH zipped AS (
  SELECT MAP_ZIP_WITH(m1, m2, (k, v1, v2) -> COALESCE(v1, 0) + COALESCE(v2, 0)) AS m
  FROM sample
)
SELECT MAP_FILTER(m, (k, v) -> v > 10) AS filtered FROM zipped;
-- Result: {b -> 22, c -> 30}
```

---

## 🔍 MAP_ZIP_WITH Behavior & Edge Cases

1. **Missing keys:** For a key in only one map, the other value is `NULL` — use `COALESCE` for arithmetic.
2. **NULL maps:** If either input map is `NULL`, the result is `NULL`.
3. **Duplicate keys:** `MAP` enforces unique keys; the last occurrence wins.
4. **Key order:** Maps are unordered — don't rely on iteration order.

### MAP_ZIP_WITH vs Alternatives

| Function | Use Case |
|----------|----------|
| `MAP_ZIP_WITH` | Per-key computations across two maps |
| `MAP_CONCAT` | Simple merge (later values overwrite) |
| `TRANSFORM_VALUES` | Transform values within one map |
| Explode + aggregate | Complex multi-map logic (higher cost) |

---

## 🧪 Real-World Patterns

### Merge Partial Day Metrics

```sql
-- m_am: {clicks -> 10, impressions -> 100}
-- m_pm: {clicks -> 12, impressions -> 90, cost -> 5.5}
SELECT MAP_ZIP_WITH(
  m_am, m_pm,
  (k, a, p) -> COALESCE(a, 0) + COALESCE(p, 0)
) AS daily
FROM events;
-- Result: {clicks -> 22, impressions -> 190, cost -> 5.5}
```

### Detect Changed Keys

```sql
WITH zipped AS (
  SELECT MAP_ZIP_WITH(m_old, m_new, (k, v1, v2) -> IF(v1 <> v2, 1, NULL)) AS flag_map
  FROM config
)
SELECT MAP_FILTER(flag_map, (k, v) -> v IS NOT NULL) AS changed_keys
FROM zipped;
```

---

## 🧠 When to Use

| Scenario | Function(s) |
|----------|------------|
| Create a map from literals | `MAP(k1, v1, …)` |
| Build map from arrays / structs | `MAP_FROM_ARRAYS`, `MAP_FROM_ENTRIES` |
| Look up a value by key | `ELEMENT_AT`, `MAP_CONTAINS_KEY` |
| Extract keys or values | `MAP_KEYS`, `MAP_VALUES`, `MAP_ENTRIES` |
| Merge two maps | `MAP_CONCAT` (simple) or `MAP_ZIP_WITH` (with logic) |
| Transform keys or values | `TRANSFORM_KEYS`, `TRANSFORM_VALUES` (see HOF section) |
| Filter map entries | `MAP_FILTER` (see HOF section) |
