# :material-filter-settings: Predicate Pushdown

Predicate pushdown means Spark applies filters **as early as possible**, ideally
at the data source. This reduces I/O by skipping irrelevant row groups or files.

---

## 📌 How It Works

1. Spark analyzes the `WHERE` clause.
2. If the data source supports pushdown, Spark sends filter predicates to the
   file reader.
3. Only matching row groups are read into Spark.

---

## 🔍 What Enables Pushdown

- Column-level filters on Parquet, ORC, and Delta
- Simple comparisons (`=`, `>`, `<`, `BETWEEN`, `IN`)
- Filter predicates on partition columns

### What Blocks Pushdown

- UDFs and non-deterministic functions
- Complex expressions around the filtered column
- Functions that must evaluate row-by-row (e.g., `rand()`)

---

## 🧪 Check Pushdown with EXPLAIN

```sql
EXPLAIN FORMATTED
SELECT * FROM sales
WHERE region = 'US' AND amount > 1000;
```

Look for a line like:

```
PushedFilters: [IsNotNull(region), EqualTo(region,US), GreaterThan(amount,1000)]
```

---

## Configuration Options

```sql
SET spark.sql.parquet.filterPushdown = true;
SET spark.sql.orc.filterPushdown = true;
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Large datasets | Prefer pushdown-friendly predicates |
| Partitioned tables | Filter on partition columns |
| UDF-heavy filters | Pre-compute columns instead |
| Performance tuning | Validate with `EXPLAIN` |

---

> **Tip:** Pushdown works best when filters are simple and avoid wrapping the
> filtered column in expressions.
