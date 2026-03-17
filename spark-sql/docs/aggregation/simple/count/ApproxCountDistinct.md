# APPROX_COUNT_DISTINCT

`APPROX_COUNT_DISTINCT` returns an approximate count of distinct values using the **HyperLogLog** algorithm — far faster and less memory-intensive than `COUNT(DISTINCT ...)` on large datasets.

---

## 📌 Syntax

```sql
APPROX_COUNT_DISTINCT(expr [, rsd])
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `expr` | Any | — | Column or expression to count distinct values of |
| `rsd` | DOUBLE | `0.05` | Relative standard deviation (accuracy target); range `(0, 1)` |

---

## 🔍 Behavior

1. **HyperLogLog algorithm** — uses a probabilistic data structure that trades perfect accuracy for dramatically reduced memory usage; memory scales with `1 / rsd²` not with cardinality.
2. **`rsd` accuracy** — the default `0.05` means results are within roughly ±5% of the true count with high probability; setting `rsd = 0.01` gives ±1% accuracy at higher memory cost.
3. **NULL excluded** — like `COUNT(DISTINCT ...)`, `NULL` values are excluded from the distinct count.
4. **Return type** — `BIGINT`.
5. **Exact vs approximate** — prefer exact `COUNT(DISTINCT ...)` when the dataset is small or precision is critical (billing, compliance); use `APPROX_COUNT_DISTINCT` when counting over millions or billions of rows.
6. **FILTER support** — from Spark 3.0+, the `FILTER (WHERE ...)` clause is supported on `APPROX_COUNT_DISTINCT`.

---

## 🧪 Practical Examples

### Setup

```sql
CREATE TABLE events (
    event_id   BIGINT,
    user_id    BIGINT,
    session_id STRING,
    page       STRING,
    region     STRING,
    event_date DATE
);

INSERT INTO events VALUES
    (1, 1001, 'sess-A', '/home',    'East',  DATE '2024-01-01'),
    (2, 1002, 'sess-B', '/product', 'West',  DATE '2024-01-01'),
    (3, 1001, 'sess-A', '/cart',    'East',  DATE '2024-01-01'),
    (4, 1003, 'sess-C', '/home',    'North', DATE '2024-01-02'),
    (5, 1002, 'sess-D', '/home',    'West',  DATE '2024-01-02'),
    (6, 1004, 'sess-E', '/product', 'East',  DATE '2024-01-03'),
    (7, 1001, 'sess-F', '/product', 'East',  DATE '2024-01-03'),
    (8, 1005, 'sess-G', '/cart',    'West',  DATE '2024-01-03');
```

### 1 — Basic approximate distinct count

```sql
SELECT
    APPROX_COUNT_DISTINCT(user_id)    AS approx_distinct_users,
    APPROX_COUNT_DISTINCT(session_id) AS approx_distinct_sessions
FROM events;
-- Result (approximate):
-- approx_distinct_users | approx_distinct_sessions
-- ----------------------|-------------------------
-- 5                     | 7
```

### 2 — Custom accuracy (lower rsd = higher precision)

```sql
SELECT
    APPROX_COUNT_DISTINCT(user_id, 0.01) AS approx_users_1pct_rsd,
    APPROX_COUNT_DISTINCT(user_id, 0.05) AS approx_users_5pct_rsd,
    APPROX_COUNT_DISTINCT(user_id, 0.20) AS approx_users_20pct_rsd
FROM events;
-- Lower rsd → higher accuracy, more memory usage.
-- For small tables the results are often identical to COUNT(DISTINCT ...).
```

### 3 — Comparison with exact `COUNT(DISTINCT)`

```sql
SELECT
    COUNT(DISTINCT user_id)                                            AS exact_distinct_users,
    APPROX_COUNT_DISTINCT(user_id)                                     AS approx_distinct_users,
    COUNT(DISTINCT user_id) - APPROX_COUNT_DISTINCT(user_id)           AS difference
FROM events;
-- For small datasets the values are typically equal.
-- On billion-row tables, APPROX_COUNT_DISTINCT is orders of magnitude faster.
```

### 4 — Approximate distinct count per group

```sql
SELECT
    region,
    APPROX_COUNT_DISTINCT(user_id)    AS approx_users,
    APPROX_COUNT_DISTINCT(session_id) AS approx_sessions,
    COUNT(*)                          AS total_events
FROM events
GROUP BY region
ORDER BY approx_users DESC;
-- Result:
-- region | approx_users | approx_sessions | total_events
-- --------|--------------|-----------------|-------------
-- East    | 3            | 3               | 4
-- West    | 3            | 3               | 3
-- North   | 1            | 1               | 1
```

### 5 — Daily active users (DAU) pipeline pattern

```sql
-- Standard pattern for DAU/MAU metrics at scale
SELECT
    event_date,
    APPROX_COUNT_DISTINCT(user_id)    AS dau,
    APPROX_COUNT_DISTINCT(session_id) AS daily_sessions
FROM events
GROUP BY event_date
ORDER BY event_date;
-- APPROX_COUNT_DISTINCT is the standard choice in DAU/MAU pipelines
-- where exact COUNT(DISTINCT ...) would require expensive dedup shuffles.
```

---

## 🧠 When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| DAU / MAU / unique visitor metrics at scale | `APPROX_COUNT_DISTINCT(user_id)` |
| Cardinality estimation for query planning | `APPROX_COUNT_DISTINCT(col)` |
| Fast exploratory data analysis | `APPROX_COUNT_DISTINCT(col)` |
| Exact count required (billing, compliance) | `COUNT(DISTINCT col)` |
| Higher accuracy needed (e.g., ±1%) | `APPROX_COUNT_DISTINCT(col, 0.01)` |
| Counting over billions of rows efficiently | `APPROX_COUNT_DISTINCT` over `COUNT(DISTINCT)` |
