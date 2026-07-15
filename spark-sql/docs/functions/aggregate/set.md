# :material-sigma: collect_set

`collect_set` collects unique values from a group into an array, removing duplicates.

### :material-sitemap: Overview

```mermaid
graph LR
    A[Input Rows] --> B[GROUP BY]
    B --> C[COLLECT_SET]
    C --> D[One Row per Group]
```

## :material-pin: Syntax

```sql
collect_set(expr)
```

- Returns: `ARRAY<T>` containing distinct values
- Removes duplicates
- Excludes NULLs
- Non-deterministic ordering

## :material-magnify: Behavior

1. Aggregates all **distinct** non-NULL values from the group into an array.
2. Duplicates are **removed** (unlike `collect_list`).
3. NULLs are excluded from the result.
4. The order of elements is non-deterministic.

## :material-flask-outline: Practical Examples

### Basic Collection

```sql
SELECT collect_set(col) FROM VALUES (1), (2), (1) AS tab(col);
-- Result: [1, 2]
```

### Grouped Distinct Collection

```sql
CREATE OR REPLACE TEMP VIEW visits AS
SELECT * FROM VALUES
  ('Alice', 'home'), ('Alice', 'products'), ('Alice', 'home'),
  ('Bob', 'home'), ('Bob', 'cart'), ('Bob', 'cart')
AS visits(user_name, page);

SELECT user_name, collect_set(page) AS unique_pages
FROM visits
GROUP BY user_name;
```

| user_name | unique_pages |
|-----------|-------------|
| Alice | [home, products] |
| Bob | [home, cart] |

### NULL Handling

```sql
SELECT collect_set(col) FROM VALUES (1), (NULL), (2), (NULL), (1) AS tab(col);
-- Result: [1, 2]  (NULLs and duplicates removed)
```

### Count Unique with SIZE

```sql
SELECT SIZE(collect_set(page)) AS distinct_page_count
FROM visits;
-- Result: 3
```

## :material-brain: When to Use

| Scenario | Function |
|----------|----------|
| Unique tags/categories per group | `collect_set` |
| All values including duplicates | `collect_list` |
| Count distinct in nested output | `SIZE(collect_set(col))` |
| Distinct comma-separated list | `CONCAT_WS(', ', collect_set(col))` |
