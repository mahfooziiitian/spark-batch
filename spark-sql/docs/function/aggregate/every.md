# every / bool_and

`every` (also known as `bool_and`) returns `true` if **all** values in the group evaluate to true.

## 📌 Syntax

```sql
every(expr)
bool_and(expr)
```

- Returns: `BOOLEAN`
- Returns `true` only if all values are true
- Returns `false` if any value is false
- NULL values are ignored (unless all values are NULL, then returns NULL)

## 🔍 Behavior

1. Evaluates to `true` when every non-NULL value in the group is `true`.
2. Evaluates to `false` if any value is `false`.
3. NULL values are skipped during evaluation.
4. `every` and `bool_and` are aliases — they behave identically.

## 🧪 Practical Examples

### All True

```sql
SELECT every(col) FROM VALUES (true), (true), (true) AS tab(col);
-- Result: true
```

### Mixed Values

```sql
SELECT every(col) FROM VALUES (true), (false), (true) AS tab(col);
-- Result: false
```

### With NULLs

```sql
SELECT every(col) FROM VALUES (true), (NULL), (true) AS tab(col);
-- Result: true  (NULLs are ignored)
```

### Grouped Validation

```sql
CREATE OR REPLACE TEMP VIEW tasks AS
SELECT * FROM VALUES
  ('Project A', true), ('Project A', true),
  ('Project B', true), ('Project B', false)
AS tasks(project, completed);

SELECT project, every(completed) AS all_completed
FROM tasks
GROUP BY project;
```

| project | all_completed |
|---------|--------------|
| Project A | true |
| Project B | false |

### Related: some / bool_or

```sql
-- `some` / `bool_or`: true if ANY value is true
SELECT some(col) FROM VALUES (false), (true), (false) AS tab(col);
-- Result: true
```

## 🧠 every vs some

| Function | Returns true when | Alias |
|----------|------------------|-------|
| `every` | All values are true | `bool_and` |
| `some` | At least one value is true | `bool_or` |
