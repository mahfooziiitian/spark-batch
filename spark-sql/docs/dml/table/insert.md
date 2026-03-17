# INSERT

`INSERT` adds rows to a table. Spark SQL supports appending rows, overwriting
entire tables or specific partitions, and inserting literal values.

---

## 📌 Syntax

### INSERT INTO (Append)

```sql
INSERT INTO [TABLE] table_name
[PARTITION (col1 [= val1], ...)]
select_statement;
```

### INSERT OVERWRITE (Replace)

```sql
INSERT OVERWRITE [TABLE] table_name
[PARTITION (col1 [= val1], ...)]
select_statement;
```

### INSERT INTO VALUES (Literal Rows)

```sql
INSERT INTO table_name [(col1, col2, ...)]
VALUES (val1, val2, ...), (val3, val4, ...);
```

| Clause | Purpose |
|--------|---------|
| `INTO` | Appends rows to existing data |
| `OVERWRITE` | Replaces data (whole table or target partitions) |
| `PARTITION` | Targets specific partition columns |
| `VALUES` | Inserts literal rows without a SELECT |

---

## 🔍 Behavior

1. **Schema matching** — Column count and types of the source must match the
   target. Column names are matched by *position*, not by name.
2. **Dynamic partition overwrite** — When `spark.sql.sources.partitionOverwriteMode`
   is `dynamic`, `INSERT OVERWRITE` replaces only the partitions present in the
   source data, leaving other partitions untouched.
3. **Static partition insert** — Providing literal values in the `PARTITION`
   clause (e.g., `PARTITION (year = 2024)`) writes all rows to that partition
   regardless of row content.
4. **Auto-create partitions** — New partition directories are created
   automatically if they do not exist.
5. **Atomicity** — On Delta tables, `INSERT` is transactional. On Hive/Parquet
   tables, a failed write may leave partial files.

---

## 🧪 Practical Examples

### Append from a Query

```sql
INSERT INTO flights_archive
SELECT * FROM flights
WHERE flight_date < '2023-01-01';
```

### Overwrite a Partition

```sql
INSERT OVERWRITE TABLE events
PARTITION (event_date = '2024-03-01')
SELECT event_id, user_id, event_type
FROM staging_events
WHERE event_date = '2024-03-01';
```

### Dynamic Partition Overwrite

```sql
SET spark.sql.sources.partitionOverwriteMode = dynamic;

INSERT OVERWRITE TABLE sales
PARTITION (region)
SELECT product, amount, region
FROM daily_sales;
-- Only partitions present in daily_sales are replaced
```

### Insert Literal Rows

```sql
INSERT INTO lookup_codes (code, description)
VALUES ('A', 'Active'),
       ('I', 'Inactive'),
       ('P', 'Pending');
```

### Multi-table Insert (Hive-style)

```sql
FROM raw_events
INSERT INTO click_events
  SELECT * WHERE event_type = 'click'
INSERT INTO view_events
  SELECT * WHERE event_type = 'view';
```

---

## 🧠 When to Use

| Scenario | Recommended Pattern |
|----------|-------------------|
| Append new records | `INSERT INTO ... SELECT` |
| Rebuild a full partition | `INSERT OVERWRITE ... PARTITION (...)` |
| Replace only touched partitions | Dynamic partition overwrite mode |
| Seed reference data | `INSERT INTO ... VALUES` |
| Split rows to multiple targets | Multi-table `FROM ... INSERT` |

---

> **Tip:** Prefer `MERGE INTO` over `INSERT` + `UPDATE` when you need
> upsert semantics on Delta tables.
