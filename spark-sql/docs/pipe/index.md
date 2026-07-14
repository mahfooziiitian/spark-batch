# :material-pipe: Pipe Syntax

!!! info "Spark 4.0"
    The pipe operator `|>` is new in Apache Spark 4.0.

The **pipe operator** (`|>`) chains query operations into a readable, top-to-bottom
pipeline — eliminating deeply nested subqueries.

---

## :material-pin: Syntax

```sql
source_relation
|> operator1
|> operator2
|> ...;
```

The source can be `FROM table`, `TABLE name`, or any standard `SELECT` query.

---

## :material-format-list-bulleted: Supported Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `SELECT` | Project columns (no aggregates) | `\|> SELECT id, name` |
| `EXTEND` | Add columns without replacing existing | `\|> EXTEND x + 1 AS y` |
| `SET` | Update an existing column in-place | `\|> SET price = price * 1.1` |
| `DROP` | Remove columns | `\|> DROP temp_col` |
| `AS` | Rename the relation | `\|> AS orders` |
| `WHERE` | Filter rows | `\|> WHERE active = true` |
| `JOIN` | Join with another table | `\|> JOIN dim ON ...` |
| `ORDER BY` | Sort results | `\|> ORDER BY created DESC` |
| `LIMIT` | Limit result count | `\|> LIMIT 100` |
| `TABLESAMPLE` | Sample rows | `\|> TABLESAMPLE (10 PERCENT)` |
| `PIVOT` | Pivot data | `\|> PIVOT (...)` |
| `UNPIVOT` | Unpivot data | `\|> UNPIVOT (...)` |

---

## :material-code-tags: Examples

### Basic Pipeline

```sql
-- Traditional nested approach
SELECT dept_id, adjusted_salary
FROM (
    SELECT dept_id, salary * 1.1 AS adjusted_salary
    FROM employees
    WHERE salary > 50000
)
ORDER BY adjusted_salary DESC;

-- Pipe syntax equivalent
FROM employees
|> WHERE salary > 50000
|> SELECT dept_id, salary * 1.1 AS adjusted_salary
|> ORDER BY adjusted_salary DESC;
```

### EXTEND — Add Columns

```sql
-- Add computed columns without losing existing ones
TABLE orders
|> EXTEND quantity * unit_price AS total
|> EXTEND total * 0.08 AS tax
|> SELECT order_id, total, tax;
```

### SET — Update Column In-Place

```sql
TABLE products
|> EXTEND 0 AS discount
|> SET discount = price * 0.1
|> SELECT name, price, discount;
```

### DROP — Remove Columns

```sql
SELECT 1 AS x, 2 AS y, 3 AS z
|> DROP z, y;
-- Result: only column x remains
```

### AS — Rename Relation

```sql
TABLE employees
|> AS emp
|> SELECT emp.id, emp.name, emp.dept_id;
```

### JOIN in Pipeline

```sql
TABLE orders
|> JOIN customers ON orders.customer_id = customers.id
|> WHERE customers.region = 'EMEA'
|> SELECT orders.order_id, customers.name, orders.total;
```

### Lateral Column Alias in Pipe SELECT

```sql
-- Reference aliases defined earlier in the same SELECT
TABLE transactions
|> SELECT
     amount * quantity AS subtotal,
     subtotal * 0.08 AS tax,
     subtotal + tax AS total;
```

---

## :material-alert-outline: Restrictions

- **No aggregate functions** in `|> SELECT` — use a subquery or CTE before piping.
- The single-character `|` (bitwise OR) may conflict — disable with:
  ```sql
  SET spark.sql.parser.singleCharacterPipeOperator.enabled = false;
  ```

---

## :material-lightbulb-outline: When to Use Pipe Syntax

| Scenario | Recommendation |
|----------|---------------|
| Multi-step transformations | :white_check_mark: Pipe makes the flow linear and readable |
| Simple single-table queries | Traditional `SELECT` is fine |
| Complex aggregations | Use CTE + pipe for the post-aggregation steps |
| Data exploration in notebooks | :white_check_mark: Pipe is great for iterative exploration |
