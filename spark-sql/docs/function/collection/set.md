# :material-set-all: Set Functions

Set functions work with **distinct collections** — aggregating unique values and searching
within delimited strings.

### :material-sitemap: Overview

```mermaid
graph LR
    A[Input] --> B[COLLECT_SET]
    B --> C[Distinct Array]
```

## 📌 COLLECT_SET

### Syntax

```sql
COLLECT_SET(expr)
```

Aggregates all non-NULL values from a group into an array of **unique** elements.

### 🔍 Behavior

1. Returns an `ARRAY<T>` containing distinct non-NULL values from the group.
2. Duplicates are **removed** (use `COLLECT_LIST` to keep them).
3. Order is **non-deterministic** after a shuffle.
4. Returns an empty array if all values are NULL.

### 🧪 Practical Examples

#### 🧱 1. Basic Distinct Collection

```sql
SELECT COLLECT_SET(col) FROM VALUES (1), (2), (1), (3), (2) AS tab(col);
-- Result: [1, 2, 3]  (order may vary)
```

#### 🧱 2. Grouped Distinct Collection

```sql
CREATE OR REPLACE TEMP VIEW purchases AS
SELECT * FROM VALUES
  ('Alice', 'laptop'), ('Alice', 'mouse'), ('Alice', 'laptop'),
  ('Bob', 'keyboard'), ('Bob', 'monitor'), ('Bob', 'keyboard')
AS purchases(customer, product);

SELECT customer, COLLECT_SET(product) AS unique_products
FROM purchases
GROUP BY customer;
-- Alice → [laptop, mouse], Bob → [keyboard, monitor]
```

#### 🧱 3. Count Distinct via Set

```sql
SELECT customer, SIZE(COLLECT_SET(product)) AS distinct_count
FROM purchases
GROUP BY customer;
-- Alice → 2, Bob → 2
```

#### 🧱 4. Sorted Distinct Values

```sql
SELECT SORT_ARRAY(COLLECT_SET(col)) AS sorted_unique
FROM VALUES (3), (1), (4), (1), (5), (3) AS tab(col);
-- Result: [1, 3, 4, 5]
```

#### 🧱 5. As Comma-Separated Distinct String

```sql
SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(col))) AS csv
FROM VALUES ('b'), ('a'), ('c'), ('a') AS tab(col);
-- Result: 'a, b, c'
```

---

## 📌 FIND_IN_SET

### Syntax

```sql
FIND_IN_SET(str, str_array)
```

Returns the **1-based index** of `str` in a comma-delimited string `str_array`.

### 🔍 Behavior

1. Returns the 1-based position if found.
2. Returns `0` if not found.
3. Returns `0` if `str` itself contains a comma.
4. Returns `NULL` if either argument is NULL.

### 🧪 Practical Examples

#### 🧱 1. Find Position in CSV String

```sql
SELECT FIND_IN_SET('ab', 'abc,b,ab,c,def');
-- Result: 3
```

#### 🧱 2. Not Found

```sql
SELECT FIND_IN_SET('xyz', 'abc,b,ab,c,def');
-- Result: 0
```

#### 🧱 3. Use in WHERE Clause

```sql
CREATE OR REPLACE TEMP VIEW users AS
SELECT * FROM VALUES
  (1, 'admin,editor'), (2, 'viewer'), (3, 'editor,admin')
AS users(id, roles);

SELECT * FROM users WHERE FIND_IN_SET('admin', roles) > 0;
-- Returns users 1 and 3
```

#### 🧱 4. Conditional Logic

```sql
SELECT id,
       CASE WHEN FIND_IN_SET('admin', roles) > 0 THEN 'Has Admin'
            ELSE 'No Admin'
       END AS admin_status
FROM users;
```

---

## 🧠 When to Use

| Scenario | Function | Why |
|----------|----------|-----|
| Aggregate distinct values per group | `COLLECT_SET` | Removes duplicates automatically |
| Count distinct in nested context | `SIZE(COLLECT_SET(...))` | Alternative to `COUNT(DISTINCT ...)` in complex queries |
| Deduplicate before joining as string | `CONCAT_WS(COLLECT_SET(...))` | Clean comma-separated output |
| Search within CSV strings | `FIND_IN_SET` | 1-based index lookup in delimited data |
| Role / tag membership checks | `FIND_IN_SET` in WHERE | Filter rows by presence in CSV column |

> **Tip:** Prefer `COLLECT_SET` over `ARRAY_DISTINCT(COLLECT_LIST(...))` — it's more
> efficient since deduplication happens during aggregation, not as a post-processing step.
