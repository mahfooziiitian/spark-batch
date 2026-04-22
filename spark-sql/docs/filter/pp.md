# :material-lightning-bolt: Predicate Pushdown

Predicate pushdown is a Catalyst optimizer technique that moves filter predicates as close to the data source as possible, minimising the bytes read from storage.

---

## :material-sitemap: Overview

### With Pushdown

```mermaid
flowchart LR
    Q[Query] --> CO[Catalyst Optimizer]
    CO --> PS[Pushdown to\nParquet / Delta scan]
    PS --> RG[Read matching\nrow groups only]
    RG --> SP[Spark processes\nsmall dataset]
```

### Without Pushdown

```mermaid
flowchart LR
    Q[Query with UDF] --> CO[Catalyst Optimizer]
    CO --> NP[Cannot push predicate]
    NP --> FS[Full scan — all data read]
    FS --> SP[Spark loads all data]
    SP --> FM[Filter applied in memory]
```

---

## :material-pin: What Enables Pushdown

Predicates using the following operators on plain column references are pushed down:

| Operator / Pattern | Example |
|-------------------|---------|
| Equality | `region = 'US'` |
| Comparison | `amount > 500`, `amount <= 1000` |
| Not equal | `status != 'cancelled'` |
| BETWEEN | `amount BETWEEN 100 AND 500` |
| IN (literal list) | `region IN ('US', 'EU')` |
| IS NULL | `region IS NULL` |
| IS NOT NULL | `score IS NOT NULL` |
| AND / OR combinations | `region = 'US' AND amount > 500` |
| Partition column filter | `year = 2024 AND month = 1` |

---

## :material-pin: What Blocks Pushdown

| Pattern | Why it blocks |
|---------|--------------|
| UDFs — `my_udf(col) = 1` | Catalyst cannot introspect UDF logic |
| Expression on column — `col + 1 > 5` | Column is wrapped; pushdown needs bare reference |
| `CAST(col AS ...)` on the left — `CAST(amount AS INT) > 500` | Wrapped expression |
| Non-deterministic functions — `RAND() < 0.5` | Varies per row; cannot be pushed |
| Python UDFs (PySpark) | Opaque to JVM Catalyst |

---

## :material-flask-outline: Examples

### :material-numeric-1-circle: Verify pushdown with EXPLAIN FORMATTED

```sql
EXPLAIN FORMATTED
SELECT order_id, amount
FROM orders
WHERE region = 'US' AND amount > 500;
-- Result (excerpt from plan output):
-- == Physical Plan ==
-- ...
-- PushedFilters: [IsNotNull(region), IsNotNull(amount),
--                EqualTo(region,US), GreaterThan(amount,500.0)]
-- ...
```

### :material-numeric-2-circle: UDF blocks pushdown

```sql
-- Define a simple UDF
CREATE OR REPLACE TEMPORARY FUNCTION is_us(r STRING) RETURNS BOOLEAN
RETURN r = 'US';

EXPLAIN FORMATTED
SELECT order_id FROM orders WHERE is_us(region);
-- Result (excerpt):
-- PushedFilters: []   -- empty: UDF blocked pushdown
```

### :material-numeric-3-circle: Expression on column blocks pushdown

```sql
EXPLAIN FORMATTED
SELECT order_id FROM orders WHERE amount * 1.1 > 1000;
-- Result (excerpt):
-- PushedFilters: []   -- empty: expression on column blocked pushdown

-- Rewrite as bare column reference to enable pushdown:
EXPLAIN FORMATTED
SELECT order_id FROM orders WHERE amount > 909.09;
-- Result (excerpt):
-- PushedFilters: [GreaterThan(amount,909.09)]
```

---

## Delta-Specific Optimisations

Delta Lake extends pushdown with file-level statistics (min/max per column) and Z-ORDER clustering:

```sql
-- Cluster the table by frequently filtered columns
OPTIMIZE orders ZORDER BY (region, status);

-- Spark now reads only Delta files whose region/status stats match the filter
SELECT order_id, amount
FROM orders
WHERE region = 'US' AND status = 'shipped';
```

Z-ORDER is most effective when filtering on two or three high-cardinality columns together.

---

## Configuration Reference

| Configuration key | Default | Effect |
|-------------------|---------|--------|
| `spark.sql.parquet.filterPushdown` | `true` | Push predicates into Parquet row-group filters |
| `spark.sql.orc.filterPushdown` | `true` | Push predicates into ORC stripe filters |
| `spark.sql.optimizer.dynamicPartitionPruning.enabled` | `true` | Prune partitions at runtime using broadcast join results |

---

## :material-brain: When to Use

| Scenario | Recommendation |
|----------|---------------|
| Querying large Parquet / Delta tables | Always filter on bare column references |
| Partition columns available | Filter on partition columns first |
| Frequently filtered columns | Apply `OPTIMIZE ... ZORDER BY` on Delta |
| UDF required in filter | Push additional bare-column predicates alongside the UDF |
| Checking if pushdown is active | Use `EXPLAIN FORMATTED` and inspect `PushedFilters` |
