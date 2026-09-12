# :material-format-text: String Operators

Spark SQL 4.2 supports operator-style string concatenation and several pattern-matching operators. The most important gotcha is that `||` and `CONCAT()` are both null-propagating in Spark SQL.

## :material-animation-play: Interactive Visualization — Concatenation and Pattern Results

<div id="viz-operator-string-null" class="ts-viz"></div>

Compare concatenation forms and pattern operators using outcomes verified directly in PySpark 4.2.

<script src="../../../assets/js/querying-operator-viz.js"></script>

______________________________________________________________________

## :material-code-tags: Syntax

| Operator / expression | Purpose                                 | Verified example                  |
| --------------------- | --------------------------------------- | --------------------------------- |
| `\|\|`                | Concatenate strings                     | `'Hello' \|\| ' ' \|\| 'World'`   |
| `LIKE`                | Case-sensitive wildcard match           | `name LIKE 'Al%'`                 |
| `NOT LIKE`            | Negated wildcard match                  | `email NOT LIKE '%@test.%'`       |
| `ILIKE`               | Case-insensitive wildcard match         | `name ILIKE 'alice%'`             |
| `NOT ILIKE`           | Negated case-insensitive wildcard match | `'Abc' NOT ILIKE 'a%'`            |
| `RLIKE`               | Java regex match                        | `phone RLIKE '^\\+[0-9]{10,15}$'` |
| `NOT RLIKE`           | Negated regex match                     | `'abc' NOT RLIKE '^[a-z]+$'`      |

______________________________________________________________________

## :material-table: Verified Spark 4.2 Outcomes

| Expression checked in PySpark 4.2                  | Result          |
| -------------------------------------------------- | --------------- |
| `'Hello' \|\| ' ' \|\| 'World'`                    | `'Hello World'` |
| `typeof('a' \|\| 'b')`                             | `string`        |
| `'a' \|\| NULL`                                    | `NULL`          |
| `CONCAT('a', NULL)`                                | `NULL`          |
| `CONCAT_WS('-', 'a', NULL, 'b')`                   | `'a-b'`         |
| `'Abc' LIKE 'a%'`                                  | `FALSE`         |
| `'Abc' ILIKE 'a%'`                                 | `TRUE`          |
| `'Abc' NOT ILIKE 'a%'`                             | `FALSE`         |
| `'abc' RLIKE '^[a-z]+$'`                           | `TRUE`          |
| `'abc' NOT RLIKE '^[a-z]+$'`                       | `FALSE`         |
| `'abc' LIKE NULL`                                  | `NULL`          |
| `'100% complete' LIKE '100\% complete'`            | `TRUE`          |
| `'A_001' LIKE 'A\_001'`                            | `TRUE`          |
| `'100% complete' LIKE '100!% complete' ESCAPE '!'` | `TRUE`          |

______________________________________________________________________

## :material-information-outline: Behavior Notes

1. `||` is fully supported in Spark SQL 4.2.
2. `||` and `CONCAT()` behave the same on `NULL`: if any operand is `NULL`, the result is `NULL`.
3. `CONCAT_WS()` differs because it skips `NULL` arguments instead of nulling the whole result.
4. `ILIKE` provides case-insensitive wildcard matching.
5. `RLIKE` uses Java regular-expression syntax.
6. Spark SQL 4.2 does not support `SIMILAR TO`; use `RLIKE` when you need regex-style matching.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Concatenation

```sql
SELECT
    first_name || ' ' || last_name AS full_name,
    '[' || CAST(order_id AS STRING) || ']' AS order_ref
FROM customers;
```

### Null-aware string building

```sql
SELECT
    first_name || ' ' || middle_name || ' ' || last_name AS null_propagating_name,
    CONCAT_WS(' ', first_name, middle_name, last_name) AS null_skipping_name
FROM employees;
```

### Wildcard patterns

```sql
SELECT * FROM customers WHERE name LIKE 'John%';
SELECT * FROM users WHERE username ILIKE 'alice%';
```

### Regex validation

```sql
SELECT *
FROM users
WHERE email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$';
```

### Escaping `%` and `_`

```sql
SELECT
    '100% complete' LIKE '100\% complete' AS percent_match,
    'A_001' LIKE 'A\_001' AS underscore_match,
    '100% complete' LIKE '100!% complete' ESCAPE '!' AS explicit_escape_match;
```

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario                              | Recommended pattern             |
| ------------------------------------- | ------------------------------- |
| Concatenate required non-null parts   | \`a                             |
| Concatenate while skipping `NULL`s    | `CONCAT_WS(sep, ...)`           |
| Case-sensitive wildcard filter        | `LIKE`                          |
| Case-insensitive wildcard filter      | `ILIKE`                         |
| Regex validation or extraction filter | `RLIKE`                         |
| Match literal `%` or `_`              | Escape with `\` or use `ESCAPE` |
