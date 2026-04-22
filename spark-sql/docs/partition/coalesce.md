# :material-table-merge-cells: Coalesce Partitions

`COALESCE` reduces the number of partitions without a full shuffle.
It is cheaper than `REPARTITION` but can lead to uneven partition sizes.

---

## :material-pin: Syntax

```sql
SELECT /*+ COALESCE(n) */ * FROM table;
```

---

## :material-magnify: Behavior

1. Coalesce avoids shuffle; it merges adjacent partitions.
2. It can create skew if data is not evenly distributed.
3. Use for reducing output file counts.

---

## :material-flask-outline: Example

```sql
SELECT /*+ COALESCE(10) */ *
FROM events
WHERE event_date = '2024-01-01';
```

---

## :material-brain: When to Use

| Scenario | Use |
|----------|-----|
| Reduce output files | `COALESCE` |
| Need full redistribution | Use `REPARTITION` |
