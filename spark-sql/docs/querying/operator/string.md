# :material-format-text: String Operators

Spark SQL provides the `||` concatenation operator and several pattern-matching
operators — `LIKE`, `ILIKE`, `RLIKE` — for building, searching, and validating
string values directly in SQL without calling a function.

---

## :material-code-tags: Syntax

| Operator | Description | Example |
|----------|-------------|---------|
| `\|\|` | Concatenate two strings | `'Hello' \|\| ' ' \|\| 'World'` → `'Hello World'` |
| `LIKE pattern` | SQL wildcard match (`%` = any chars, `_` = one char) | `name LIKE 'Al%'` |
| `NOT LIKE pattern` | Inverse wildcard match | `email NOT LIKE '%@test.%'` |
| `ILIKE pattern` | Case-insensitive `LIKE` | `name ILIKE 'alice%'` |
| `NOT ILIKE pattern` | Inverse case-insensitive match | |
| `RLIKE pattern` | Java regex match | `phone RLIKE '^\\+[0-9]{10,15}$'` |
| `NOT RLIKE pattern` | Inverse regex match | |

---

## :material-information-outline: Behavior

1. `||` propagates `NULL` — if either side is `NULL`, the result is `NULL`. Use `CONCAT_WS` or `COALESCE` to handle nulls in concatenation.
2. `LIKE` `%` matches **zero or more** characters; `_` matches **exactly one** character.
3. `ILIKE` is only available in Spark SQL 3.3+ (and Databricks). It is equivalent to `LOWER(col) LIKE LOWER(pattern)` but more efficient.
4. `RLIKE` uses Java's `java.util.regex` syntax — remember to double-escape backslashes: `\\d` for a digit class.
5. Pattern operators return `NULL` when either the column or the pattern is `NULL`.
6. For prefix `LIKE` patterns (e.g., `'abc%'`), Catalyst can push the filter to the storage reader as a range predicate. Infix patterns (`'%abc%'`) cannot be pushed.

---

## :material-flask-outline: Practical Examples

### String concatenation with `||`

```sql
SELECT
    first_name || ' ' || last_name              AS full_name,
    city       || ', ' || country               AS location,
    '[' || CAST(order_id AS STRING) || ']'      AS order_ref
FROM customers;
```

### NULL-safe concatenation

```sql
-- || returns NULL if middle_name is NULL
SELECT first_name || ' ' || middle_name || ' ' || last_name AS full_name
FROM employees;

-- Use CONCAT_WS to skip NULLs
SELECT CONCAT_WS(' ', first_name, middle_name, last_name) AS full_name
FROM employees;
```

### LIKE — prefix, suffix, contains, single character

```sql
-- Prefix: names starting with 'John'
SELECT * FROM customers WHERE name LIKE 'John%';

-- Suffix: email ending with '.org'
SELECT * FROM contacts WHERE email LIKE '%.org';

-- Contains: product name includes 'Pro'
SELECT * FROM products WHERE name LIKE '%Pro%';

-- Single-char wildcard: 3-letter codes like 'U_A'
SELECT * FROM airports WHERE iata_code LIKE 'U_A';
```

### Escape special LIKE characters

```sql
-- Match a literal '%' by escaping with backslash
SELECT * FROM notes WHERE content LIKE '%100\% complete%' ESCAPE '\';

-- Match a literal '_'
SELECT * FROM codes WHERE code LIKE 'A\_001' ESCAPE '\';
```

### ILIKE — case-insensitive search

```sql
-- Matches 'alice', 'Alice', 'ALICE', 'aLiCe'
SELECT * FROM users WHERE username ILIKE 'alice%';

-- Case-insensitive contains
SELECT * FROM articles WHERE title ILIKE '%spark sql%';
```

### RLIKE — regex patterns

```sql
-- US phone number formats: (555) 123-4567 or 555-123-4567 or 5551234567
SELECT phone
FROM contacts
WHERE phone RLIKE '^(\\(\\d{3}\\)\\s?|\\d{3}[-.]?)\\d{3}[-.]?\\d{4}$';

-- Valid email (basic)
SELECT email
FROM users
WHERE email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$';

-- Extract rows where code contains only uppercase letters and digits
SELECT * FROM products WHERE sku RLIKE '^[A-Z0-9]+$';

-- Starts with a digit
SELECT * FROM ids WHERE identifier RLIKE '^[0-9]';
```

### NOT LIKE — exclusion patterns

```sql
-- Exclude test and example emails
SELECT * FROM users
WHERE email NOT LIKE '%@test.%'
  AND email NOT LIKE '%@example.%'
  AND email NOT LIKE '%+test%';
```

### Combine `||` with CASE WHEN

```sql
SELECT
    order_id,
    CASE WHEN is_urgent THEN '[URGENT] ' ELSE '' END || subject AS display_subject
FROM support_tickets;
```

### Build a dynamic label with `||`

```sql
SELECT
    product_id,
    name || ' (' || category || ') — $' || CAST(ROUND(price, 2) AS STRING) AS product_label
FROM products;
```

---

## :material-lightbulb-outline: When to Use

| Scenario | Pattern |
|----------|---------|
| Build a display string from columns | `col1 \|\| sep \|\| col2` |
| NULL-safe string building | `CONCAT_WS(sep, col1, col2, ...)` |
| Prefix / suffix / contains filter | `LIKE` with `%` |
| Case-insensitive text search | `ILIKE` (Spark 3.3+) |
| Complex pattern validation (email, phone) | `RLIKE` (Java regex) |
| Exclude known bad patterns | `NOT LIKE` / `NOT RLIKE` |
| Escape literal `%` or `_` in LIKE | `LIKE 'val\%' ESCAPE '\'` |
