# Hint Resolver

Welcome to the **Hint Resolver** tutorial!  
This guide explains how Spark SQL processes and resolves join hints to optimize query execution.

---

## Introduction

Join hints in Spark SQL help the optimizer choose the most efficient join strategy. Understanding how hints are resolved can improve query performance.

## How Hint Resolution Works

- Spark parses SQL queries and identifies join hints.
- The optimizer applies these hints during the logical plan analysis.
- Supported hints include `BROADCAST`, `MERGE`, `SHUFFLE_HASH`, and more.

## Examples

```sql
SELECT /*+ BROADCAST(t1) */ *
FROM table1 t1
JOIN table2 t2 ON t1.id = t2.id
```

## Best Practices

- Use hints judiciously; unnecessary hints may degrade performance.
- Test and benchmark queries with and without hints.

## References

- [Spark SQL Documentation](https://spark.apache.org/docs/latest/sql-performance-tuning.html#join-strategies)
- [Join Hints in Spark SQL](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html)

---
