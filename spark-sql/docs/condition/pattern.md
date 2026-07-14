# :material-regex: Pattern Matching Conditions

Pattern predicates filter string columns using wildcards or full regular expressions.
Spark SQL provides `LIKE`, `ILIKE` (case-insensitive), and `RLIKE` (Java regex).

---

## :material-pin: Operator Reference

| Operator | Syntax | Case-sensitive | Description |
|----------|--------|:--------------:|-------------|
| `LIKE` | `col LIKE 'A%'` | Yes | `%` = any sequence, `_` = single char |
| `NOT LIKE` | `col NOT LIKE '%test%'` | Yes | Negated wildcard match |
| `ILIKE` | `col ILIKE 'a%'` | **No** | Case-insensitive LIKE (Spark / Databricks) |
| `NOT ILIKE` | `col NOT ILIKE '%admin%'` | **No** | Case-insensitive negated LIKE |
| `RLIKE` | `col RLIKE '^[A-Z]{3}'` | Yes (by default) | Java regex match |
| `NOT RLIKE` | `col NOT RLIKE '\\d+'` | Yes | Negated regex match |
| `LIKE … ESCAPE` | `col LIKE '%\%%' ESCAPE '\'` | Yes | Match literal `%` or `_` |

---

## :material-magnify: Behavior Notes

1. **`LIKE` is case-sensitive** — `'alice' LIKE 'A%'` returns FALSE. Use `ILIKE` or `LOWER(col) LIKE 'a%'`.
2. **`%` matches zero or more characters** — `'LIKE '%'` matches any string including empty.
3. **`_` matches exactly one character** — `'LIKE 'A_'` matches `'AB'` but not `'A'`.
4. **`RLIKE` uses Java regex syntax** — `\d`, `\w`, `(?i)` for inline case-insensitive.
5. **NULLs** — `NULL LIKE '%'` returns NULL, which filters out the row.
6. **Performance** — leading `%` wildcards prevent partition pruning and push-down; avoid when possible.

---

## :material-flask-outline: Examples

### Prefix match (indexable)

```sql
-- Leading literal — partition pruning possible
SELECT * FROM users WHERE email LIKE 'alice%';
```

### Suffix match

```sql
SELECT * FROM files WHERE filename LIKE '%.parquet';
```

### Substring match

```sql
SELECT * FROM logs WHERE message LIKE '%ERROR%';
```

### Single-character wildcard

```sql
-- Matches 'Jan', 'Feb', 'Mar' … (exactly 3 chars)
SELECT * FROM calendar WHERE month_code LIKE '___';
```

### Case-insensitive match with ILIKE

```sql
-- Matches 'Admin', 'admin', 'ADMIN', 'AdMiN'
SELECT * FROM users WHERE role ILIKE 'admin';
```

### Case-insensitive fallback with LOWER

```sql
-- Portable alternative when ILIKE is unavailable
SELECT * FROM users WHERE LOWER(role) LIKE 'admin%';
```

### Escape literal wildcards

```sql
-- Match strings containing a literal percent sign
SELECT * FROM products WHERE description LIKE '%10\%%' ESCAPE '\';

-- Match strings containing a literal underscore
SELECT * FROM codes WHERE code LIKE 'A\_B' ESCAPE '\';
```

### Regex — structured ID validation

```sql
-- Match IDs like 'ABC-1234'
SELECT * FROM events WHERE event_id RLIKE '^[A-Z]{3}-[0-9]{4}$';
```

### Regex — email basic check

```sql
SELECT * FROM users WHERE email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$';
```

### Regex — inline case-insensitive flag

```sql
-- (?i) makes the whole regex case-insensitive
SELECT * FROM users WHERE name RLIKE '(?i)^alice';
```

### Regex — extract domain from URL (with regexp_extract)

```sql
SELECT url, regexp_extract(url, 'https?://([^/]+)', 1) AS domain
FROM page_views;
```

### NOT LIKE — exclude patterns

```sql
SELECT * FROM events WHERE event_type NOT LIKE '%_test' AND event_type NOT LIKE '%_debug';
```

---

## :material-speedometer: Performance Considerations

| Pattern | Pushdown? | Notes |
|---------|:---------:|-------|
| `LIKE 'prefix%'` | Partial | Literal prefix allows Parquet/Delta row-group skipping |
| `LIKE '%suffix'` | No | Full scan — leading `%` prevents pushdown |
| `LIKE '%substring%'` | No | Full scan |
| `ILIKE '…'` | No | Full scan |
| `RLIKE '…'` | No | Full scan — regex always requires full evaluation |

!!! tip "Partition column filtering"
    If the column is a partition column, partition pruning applies regardless of the pattern type.
    For non-partition columns, prefer exact `=` or `IN` over `LIKE` when the full value is known.

---

## :material-alert-circle: Common Mistakes

| Mistake | Problem | Fix |
|---------|---------|-----|
| `LIKE '%value%'` on large table | Full scan, no pushdown | Add partition filter alongside it |
| `LIKE 'A%'` when data is mixed-case | Misses `'alice'` | Use `ILIKE 'A%'` or `LOWER(col) LIKE 'a%'` |
| Literal `%` in pattern | Matches any string | Use `ESCAPE` to match literal `%` |
| Java regex in RLIKE without escaping | `\d` → must be `\\d` in SQL string | Double-escape: `'\\d+'` |
| `NULL LIKE '%'` | Returns NULL, not TRUE | Guard with `col IS NOT NULL` |

---

## :material-brain: When to Use

| Scenario | Pattern |
|----------|---------|
| Simple wildcard, known case | `LIKE` |
| Case-insensitive match | `ILIKE` (Spark) or `LOWER(col) LIKE …` |
| Structured format validation | `RLIKE` |
| Exclude patterns | `NOT LIKE` / `NOT RLIKE` |
| Literal `%` or `_` in pattern | `LIKE … ESCAPE '\'` |
| Extract part of a match | `regexp_extract(col, pattern, group)` |


Pattern predicates filter strings based on partial matches or regular
expressions.

---

## :material-pin: Operators

| Operator | Example | Description |
|----------|---------|-------------|
| `LIKE` | `name LIKE 'A%'` | `%` = many chars, `_` = one char |
| `RLIKE` | `name RLIKE '^A.*'` | Regex match |
| `NOT LIKE` | `name NOT LIKE '%test%'` | Negated pattern |

---

## :material-magnify: Behavior Notes

1. `LIKE` is case-sensitive by default.
2. Use `ESCAPE` to match literal `%` or `_` characters.
3. `RLIKE` uses Java regex syntax.

---

## :material-flask-outline: Practical Examples

### Prefix Match

```sql
SELECT * FROM users
WHERE name LIKE 'A%';
```

### Suffix Match

```sql
SELECT * FROM files
WHERE filename LIKE '%.csv';
```

### Escape Wildcards

```sql
SELECT * FROM logs
WHERE message LIKE '%\_%' ESCAPE '\';
```

### Regex Match

```sql
SELECT * FROM events
WHERE event_id RLIKE '^[A-Z]{3}-[0-9]{4}$';
```

---

## :material-brain: When to Use

| Scenario | Pattern |
|----------|---------|
| Simple wildcard matching | `LIKE` |
| Complex regex | `RLIKE` |
| Literal `%` or `_` | `ESCAPE` with `LIKE` |

---

> **Tip:** Prefer exact matches when possible; regex can be expensive.
