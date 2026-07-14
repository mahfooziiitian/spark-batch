# :material-translate: String Collation

!!! info "Spark 4.0"
    ICU-backed string collation is new in Apache Spark 4.0.

**Collations** control how strings are compared, sorted, grouped, and matched.
Prior to Spark 4.0 all strings used binary UTF-8 comparison. Now you can choose
case-insensitive, accent-insensitive, or locale-specific collation rules.

---

## :material-pin: Built-in Collations

| Collation | Case | Accent | Notes |
|-----------|:----:|:------:|-------|
| `UTF8_BINARY` | Sensitive | Sensitive | **Default** — byte-level comparison |
| `UTF8_LCASE` | Insensitive | Sensitive | Fast lowercase folding |
| `UNICODE` | Sensitive | Sensitive | ICU Unicode default |
| `UNICODE_CI` | Insensitive | Sensitive | ICU case-insensitive |
| `UNICODE_AI` | Sensitive | Insensitive | ICU accent-insensitive |
| `UNICODE_CI_AI` | Insensitive | Insensitive | Both-insensitive |
| `en`, `de`, `sv`, `tr_CI`, … | Locale-specific | Varies | Full ICU locale collations |

### Discovering Available Collations

```sql
SHOW COLLATIONS;
SHOW COLLATIONS LIKE 'UNICODE*';
```

---

## :material-code-tags: Column-Level Collation

```sql
CREATE TABLE users (
    name  STRING COLLATE UTF8_LCASE,
    email STRING COLLATE UNICODE_CI
) USING PARQUET;

-- Queries are automatically case-insensitive on these columns
SELECT * FROM users WHERE name = 'alice';    -- matches 'Alice', 'ALICE'
SELECT * FROM users WHERE email = 'BOB@X.COM'; -- matches 'bob@x.com'
```

---

## :material-code-tags: Inline Collation

Apply collation to a specific expression without changing the column definition:

```sql
-- Case-insensitive comparison on a literal
SELECT * FROM products
WHERE name = 'widget' COLLATE UTF8_LCASE;

-- Accent-insensitive search
SELECT * FROM articles
WHERE title COLLATE UNICODE_AI LIKE '%cafe%';  -- matches 'café'
```

---

## :material-earth: Locale-Specific Collation

Different languages have different sorting rules:

```sql
-- Swedish: Ö sorts after Z
SELECT 'Kypper' COLLATE sv < 'Köpfe';  -- true

-- Turkish: dotted I handling
SELECT 'I' COLLATE tr_ci = 'ı';  -- true

-- German phonebook ordering
SELECT * FROM names ORDER BY name COLLATE de;
```

---

## :material-group: Collation in GROUP BY and Set Operations

```sql
-- AAA and aaa become one group under UTF8_LCASE
SELECT name COLLATE UTF8_LCASE, COUNT(*)
FROM users
GROUP BY name COLLATE UTF8_LCASE;

-- Set operations respect collation
SELECT col1 COLLATE UTF8_LCASE FROM VALUES ('aaa'), ('AAA')
EXCEPT
SELECT col1 COLLATE UTF8_LCASE FROM VALUES ('aaa');
-- Result: empty (AAA == aaa under UTF8_LCASE)
```

---

## :material-compare-horizontal: Collation Comparison

```sql
-- Binary (default): 'ABC' ≠ 'abc'
SELECT 'ABC' = 'abc';                         -- false

-- Case-insensitive: 'ABC' = 'abc'
SELECT 'ABC' COLLATE UTF8_LCASE = 'abc';      -- true

-- Unicode case-insensitive
SELECT 'straße' COLLATE UNICODE_CI = 'STRASSE'; -- true
```

---

## :material-lightbulb-outline: Best Practices

| Scenario | Recommended Collation |
|----------|----------------------|
| Exact match (IDs, codes) | `UTF8_BINARY` (default) |
| Case-insensitive search | `UTF8_LCASE` |
| International text sorting | `UNICODE` or locale-specific |
| Email/username matching | `UNICODE_CI` |
| Accent-insensitive search | `UNICODE_AI` or `UNICODE_CI_AI` |
