# :material-check-all: Data Validation

Spark SQL provides functions for **type validation**, **safe casting**, and **pattern matching**
to ensure data quality before processing.

## :material-pin: CAST & TRY_CAST

### CAST — Strict Type Conversion

```sql
SELECT CAST('123' AS INT);        -- 123
SELECT CAST('2024-01-15' AS DATE); -- 2024-01-15
SELECT CAST('abc' AS INT);        -- Error (in ANSI mode) or NULL
```

### TRY_CAST — Safe Type Conversion

Returns `NULL` instead of raising an error when conversion fails.

```sql
SELECT TRY_CAST('123' AS INT);    -- 123
SELECT TRY_CAST('abc' AS INT);    -- NULL
SELECT TRY_CAST('99999999999' AS INT); -- NULL (overflow)
```

### Validate a Column

```sql
SELECT value,
       TRY_CAST(value AS INT) IS NOT NULL AS is_valid_int
FROM VALUES ('123'), ('abc'), ('45.6'), (NULL) AS t(value);
-- 123 → true, abc → false, 45.6 → false, NULL → NULL
```

## �� Regex Validation — RLIKE / REGEXP

### Syntax

```sql
string RLIKE pattern
string REGEXP pattern   -- alias
REGEXP_LIKE(string, pattern)
```

### :material-flask-outline: Practical Examples

#### Validate Email Format

```sql
SELECT email,
       email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$' AS valid_email
FROM VALUES ('alice@example.com'), ('bad-email'), ('bob@co.uk') AS t(email);
-- true, false, true
```

#### Validate Phone Number

```sql
SELECT phone,
       phone RLIKE '^\\+?[0-9]{10,15}$' AS valid_phone
FROM VALUES ('+1234567890'), ('abc'), ('9876543210') AS t(phone);
-- true, false, true
```

#### Validate Date Format (yyyy-MM-dd)

```sql
SELECT dt,
       dt RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' AS valid_format
FROM VALUES ('2024-01-15'), ('15/01/2024'), ('not-a-date') AS t(dt);
-- true, false, false
```

## :material-pin: IS NULL / IS NOT NULL

```sql
SELECT * FROM data WHERE value IS NOT NULL;

-- Count NULLs
SELECT COUNT(*) - COUNT(col) AS null_count FROM data;
```

## :material-pin: TYPEOF — Inspect Runtime Type

```sql
SELECT TYPEOF(1);           -- int
SELECT TYPEOF(1.0);         -- double
SELECT TYPEOF('hello');     -- string
SELECT TYPEOF(ARRAY(1, 2)); -- array<int>
```

## :material-pin: Data Quality Patterns

### Flag Invalid Rows

```sql
SELECT *,
       TRY_CAST(age AS INT) IS NULL AS invalid_age,
       email RLIKE '^.+@.+\\..+$'  AS valid_email
FROM raw_data;
```

### Filter to Valid Rows Only

```sql
SELECT * FROM raw_data
WHERE TRY_CAST(age AS INT) IS NOT NULL
  AND email RLIKE '^.+@.+\\..+$';
```

### Count Invalid Records per Column

```sql
SELECT
  SUM(CASE WHEN TRY_CAST(age AS INT) IS NULL THEN 1 ELSE 0 END) AS bad_age,
  SUM(CASE WHEN NOT email RLIKE '^.+@.+\\..+$' THEN 1 ELSE 0 END) AS bad_email
FROM raw_data;
```

## :material-brain: When to Use

| Scenario | Function |
|----------|----------|
| Convert types safely | `TRY_CAST` |
| Validate string patterns | `RLIKE` / `REGEXP` |
| Check for NULLs | `IS NULL` / `IS NOT NULL` |
| Inspect column type at runtime | `TYPEOF` |
| Flag bad records | `TRY_CAST(...) IS NULL` |
| Filter to clean data only | Combine `TRY_CAST` + `RLIKE` in WHERE |

> **Tip:** Always use `TRY_CAST` instead of `CAST` when processing untrusted data —
> it prevents query failures from bad values.
