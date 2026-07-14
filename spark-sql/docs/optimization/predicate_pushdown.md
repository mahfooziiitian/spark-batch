# :material-filter-arrow-down: Predicate Pushdown

Predicate pushdown moves filter conditions as close to the data source as possible —
ideally into the storage reader itself — so that irrelevant rows and columns are never
loaded into Spark's memory.

---

## :material-information-outline: Behavior

1. Spark's Catalyst optimizer automatically identifies `WHERE` predicates that can be
   evaluated by the underlying data source and embeds them in the scan operator.
2. For **Parquet** and **ORC**, filters on primitive types (`=`, `>`, `<`, `>=`, `<=`,
   `IN`, `IS NULL`, `IS NOT NULL`) are pushed to the column reader — entire row groups
   are skipped when their min/max statistics exclude the predicate.
3. For **Delta** tables, data skipping uses per-file statistics stored in the transaction
   log to skip entire files before they are opened.
4. For **JDBC** sources, predicates are translated into SQL `WHERE` clauses and sent to
   the remote database.
5. Wrapping a column in a **function call** (`YEAR(order_date) = 2024`, `UPPER(name) = 'BOB'`)
   prevents pushdown — the function result cannot be compared to file-level statistics.
6. **Partition pruning** is a special case of predicate pushdown: filters on partition
   columns cause entire partition directories to be skipped before any file is opened.
7. `EXPLAIN` output confirms pushdown via the `PushedFilters` field on the `FileScan` node.

---

## :material-code-tags: Verifying Pushdown

```sql
EXPLAIN
SELECT order_id, amount
FROM sales
WHERE order_date = '2024-06-01'
  AND region = 'EU';
```

Look for `PushedFilters` in the plan:

```
FileScan parquet [order_id#1, amount#4, order_date#7, region#8]
  PushedFilters: [IsNotNull(order_date), EqualTo(order_date,2024-06-01),
                  IsNotNull(region), EqualTo(region,EU)]
  ReadSchema: struct<order_id:bigint,amount:decimal(18,2)>
  PartitionFilters: [isnotnull(region#8), (region#8 = EU)]
```

- `PushedFilters` — applied inside the Parquet reader (row-group skipping).
- `PartitionFilters` — partition directory skipping (files never opened).
- `ReadSchema` — column pruning: only `order_id` and `amount` are read from disk.

---

## :material-flask-outline: Practical Examples

### Pushdown-friendly filter (direct column comparison)

```sql
-- GOOD: Predicate pushed to Parquet reader
SELECT *
FROM orders
WHERE order_date >= '2024-01-01'
  AND status = 'SHIPPED';
```

### Partition pruning

```sql
-- GOOD: Table is PARTITIONED BY (region, order_date)
-- Spark skips all directories except region=EU/order_date=2024-06-01/
SELECT order_id, amount
FROM sales
WHERE region = 'EU'
  AND order_date = '2024-06-01';
```

### Function on column — pushdown disabled

```sql
-- BAD: YEAR() wraps the column — Spark cannot push this to the reader
SELECT * FROM orders WHERE YEAR(order_date) = 2024;

-- GOOD: Rewrite as a range predicate — pushdown works
SELECT * FROM orders
WHERE order_date >= '2024-01-01'
  AND order_date <  '2025-01-01';
```

### LIKE pushdown (prefix only)

```sql
-- GOOD: Prefix LIKE is pushed (GreaterThanOrEqual / LessThan on strings)
SELECT * FROM customers WHERE name LIKE 'Sm%';

-- BAD: Infix LIKE is NOT pushed — full column scan required
SELECT * FROM customers WHERE name LIKE '%smith%';
```

### IN list pushdown

```sql
-- GOOD: Small IN list is pushed as EqualTo predicates
SELECT * FROM products WHERE category IN ('Electronics', 'Books', 'Toys');

-- BAD: Large IN lists (> spark.sql.optimizer.inSetConversionThreshold, default 10)
-- are converted to a HashSet filter — still applied early but not pushed to file reader
```

### JDBC predicate pushdown

```sql
-- Spark translates the WHERE clause and sends it to PostgreSQL
SELECT customer_id, email
FROM jdbc_customers           -- defined with FORMAT 'jdbc'
WHERE country = 'DE'
  AND signup_date >= '2023-01-01';
-- → PostgreSQL executes: WHERE country = 'DE' AND signup_date >= '2023-01-01'
```

### Delta data skipping

```sql
-- Delta reads min/max stats from the transaction log to skip files
-- where no row satisfies amount > 10000
SELECT order_id, amount
FROM delta_sales
WHERE amount > 10000;
```

Verify with:

```sql
EXPLAIN
SELECT order_id, amount FROM delta_sales WHERE amount > 10000;
-- Look for: PushedFilters: [IsNotNull(amount), GreaterThan(amount,10000.0)]
```

---

## :material-shield-outline: Common Anti-Patterns

| Anti-pattern | Problem | Fix |
|-------------|---------|-----|
| `WHERE YEAR(col) = 2024` | Function wraps column, disables pushdown | `WHERE col BETWEEN '2024-01-01' AND '2024-12-31'` |
| `WHERE CAST(col AS STRING) = '123'` | Cast disables pushdown | Store as correct type; use `WHERE col = 123` |
| `WHERE col + 0 = 5` | Arithmetic wraps column | `WHERE col = 5` |
| `WHERE LOWER(name) = 'bob'` | String function wraps column | Store pre-lowercased or use collation |
| Joining then filtering | Filter happens after join | Move filter to a CTE before the join |

---

## :material-lightbulb-outline: When to Check Pushdown

| Scenario | Action |
|----------|--------|
| Query on large Parquet/ORC table is slow | Run `EXPLAIN` and verify `PushedFilters` is populated |
| Filter on date column not using partition pruning | Confirm `PartitionFilters` appears in `EXPLAIN` output |
| Rewriting a filter to a date range | Re-run `EXPLAIN` and compare scan sizes |
| JDBC query returning too much data | Add `.option("pushDownPredicate", "true")` (default) and verify |

!!! tip "Delta Z-Order amplifies predicate pushdown"
    On Delta tables, combine predicate pushdown with `OPTIMIZE ... ZORDER BY (col)` to
    co-locate rows with similar values in the same files. This maximises the number of
    files skipped when filtering on that column.
