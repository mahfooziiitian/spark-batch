# :material-regex: Pattern Matching Conditions

Pattern predicates filter string values with wildcards or regular expressions.
Spark SQL supports `LIKE`, `ILIKE`, and `RLIKE`, each with different case-sensitivity and syntax rules.

## :material-animation-play: Interactive Visualization — Pattern Matcher Playground

<div id="viz-pattern" class="ts-viz"></div>

Swap operators, sample strings, and patterns to see wildcard matching, escaping, and case-sensitive versus case-insensitive behavior.

<script src="../../../assets/js/querying-condition-viz.js"></script>

______________________________________________________________________

## :material-pin: Operator Reference

| Operator          | Syntax                       | Case-sensitive | Notes                                                   |
| ----------------- | ---------------------------- | -------------- | ------------------------------------------------------- |
| `LIKE`            | `col LIKE 'A%'`              | Yes            | `%` matches zero or more chars, `_` matches exactly one |
| `NOT LIKE`        | `col NOT LIKE '%test%'`      | Yes            | Negated wildcard match                                  |
| `ILIKE`           | `col ILIKE 'a%'`             | No             | Case-insensitive variant supported by Spark 4.2         |
| `RLIKE`           | `col RLIKE '^[A-Z]{3}$'`     | Yes by default | Java regex syntax                                       |
| `NOT RLIKE`       | `col NOT RLIKE '\\d+'`       | Yes by default | Negated regex match                                     |
| `LIKE ... ESCAPE` | `col LIKE '%\%%' ESCAPE '\'` | Yes            | Escape `%` or `_` when you need literals                |

______________________________________________________________________

## :material-magnify: Verified PySpark 4.2 Behavior

| Expression checked in PySpark 4.2 | Result  |
| --------------------------------- | ------- |
| `'alice' LIKE 'A%'`               | `FALSE` |
| `'alice' ILIKE 'A%'`              | `TRUE`  |
| `'' LIKE '%'`                     | `TRUE`  |
| `'A' LIKE 'A_'`                   | `FALSE` |
| `'AB' LIKE 'A_'`                  | `TRUE`  |
| `'10%' LIKE '%\%%' ESCAPE '\'`    | `TRUE`  |
| `'A_B' LIKE 'A\_B' ESCAPE '\'`    | `TRUE`  |
| `'abc' RLIKE '^[A-Z]{3}$'`        | `FALSE` |
| `'abc' RLIKE '(?i)^[A-Z]{3}$'`    | `TRUE`  |
| `NULL LIKE '%'`                   | `NULL`  |

!!! note "Wildcard reminders"

    `%` can match an empty string, while `_` requires exactly one character.
    That is why `'' LIKE '%'` is `TRUE`, but `'A' LIKE 'A_'` is `FALSE`.

______________________________________________________________________

## :material-tune: Casting and ANSI Mode

PySpark 4.2 also accepted non-string input when Spark could cast it to a string.
`SELECT 123 LIKE '1%'` returned `TRUE` with ANSI both off and on.

!!! note "ANSI relevance"

    ANSI mode did not change the pattern-matching results checked on this page.
    Its bigger impact is on invalid casts in other contexts, such as string-to-number comparisons.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Prefix match

```sql
SELECT *
FROM users
WHERE email LIKE 'alice%';
```

### Suffix match

```sql
SELECT *
FROM files
WHERE filename LIKE '%.parquet';
```

### Case-insensitive match

```sql
SELECT *
FROM users
WHERE role ILIKE 'admin';
```

### Portable case-insensitive fallback

```sql
SELECT *
FROM users
WHERE LOWER(role) LIKE 'admin%';
```

### Escape literal wildcards

```sql
SELECT *
FROM products
WHERE description LIKE '%10\%%' ESCAPE '\';
```

```sql
SELECT *
FROM codes
WHERE code LIKE 'A\_B' ESCAPE '\';
```

### Regex validation

```sql
SELECT *
FROM events
WHERE event_id RLIKE '^[A-Z]{3}-[0-9]{4}$';
```

### Regex with inline case-insensitive flag

```sql
SELECT *
FROM users
WHERE name RLIKE '(?i)^alice';
```

______________________________________________________________________

## :material-speedometer: Performance Notes

- `LIKE 'prefix%'` is usually easier for a data source to optimize than patterns that start with `%`.
- `RLIKE` is the most expressive option, but it typically requires full value evaluation.
- Use `EXPLAIN` if pushdown matters; exact optimization depends on the source, format, and connector.

______________________________________________________________________

## :material-alert-circle: Common Mistakes

| Mistake                              | Problem                                     | Fix                                               |
| ------------------------------------ | ------------------------------------------- | ------------------------------------------------- |
| Assuming `LIKE` ignores case         | It does not                                 | Use `ILIKE` or `LOWER(col) LIKE ...`              |
| Forgetting to escape `%` or `_`      | Wildcards match more than intended          | Add `ESCAPE '\'` and escape the literal character |
| Writing Java regex escapes only once | SQL string parsing consumes one layer       | Use `'\\\\d+'`, not `'\\d+'`                      |
| Ignoring `NULL` input                | `NULL LIKE ...` returns `NULL`, not `FALSE` | Guard with `col IS NOT NULL` when needed          |

______________________________________________________________________

## :material-brain: When to Use

| Scenario                           | Pattern               |
| ---------------------------------- | --------------------- |
| Simple wildcard match              | `LIKE`                |
| Case-insensitive wildcard match    | `ILIKE`               |
| Structured format validation       | `RLIKE`               |
| Literal `%` or `_` in the pattern  | `LIKE ... ESCAPE`     |
| Portable case-insensitive matching | `LOWER(col) LIKE ...` |
