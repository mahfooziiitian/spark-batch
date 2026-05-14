# :material-folder-edit-outline: Partition Management

Partition management commands add, drop, rename, and repair partition metadata without
rewriting data. They operate at the metastore level and are supported on both managed and
external tables.

---

## :material-code-tags: Syntax

```sql
-- Add one or more partitions
ALTER TABLE table_name ADD [IF NOT EXISTS]
    PARTITION (part_col = 'val' [, ...])
    [LOCATION 'path']
    [PARTITION (part_col = 'val' [, ...]) [LOCATION 'path']]
    ...;

-- Drop one or more partitions
ALTER TABLE table_name DROP [IF EXISTS]
    PARTITION (part_col = 'val' [, ...])
    [, PARTITION (part_col = 'val' [, ...])]
    [PURGE];

-- Rename a partition (change its metadata key, not a data move)
ALTER TABLE table_name
    PARTITION (part_col = 'old_val')
    RENAME TO PARTITION (part_col = 'new_val');

-- Register all untracked partition directories (non-Delta only)
MSCK REPAIR TABLE table_name;

-- Set a custom storage location for an existing partition
ALTER TABLE table_name
    PARTITION (part_col = 'val')
    SET LOCATION 'new_path';
```

| Command | Effect on data | Effect on metastore |
|---------|---------------|---------------------|
| `ADD PARTITION` | None | Registers a new partition entry |
| `DROP PARTITION` | Deletes data files (managed) / leaves data (external) | Removes partition entry |
| `DROP PARTITION PURGE` | Bypasses Trash; permanent delete | Removes partition entry |
| `RENAME TO PARTITION` | None (metadata only) | Updates partition key value |
| `MSCK REPAIR TABLE` | None | Adds all unregistered directories as partitions |
| `SET LOCATION` | None | Points partition to a different path |

---

## :material-information-outline: Behavior

1. `ADD PARTITION` registers the partition in the metastore but does **not** create the directory or move any data — data must already exist at the path.
2. `DROP PARTITION` on a **managed** table deletes the underlying files. On an **external** table it only removes the metastore record; files remain.
3. `PURGE` skips the Trash/recycle mechanism — dropped data cannot be recovered.
4. `MSCK REPAIR TABLE` scans the table's root storage path for `key=value` directories not yet in the metastore and registers them in bulk. It is a no-op for Delta tables.
5. `RENAME TO PARTITION` renames the directory on the filesystem **and** updates the metastore record — the physical folder is moved.
6. Partition management commands acquire a table-level lock; concurrent writes may be blocked briefly.

!!! note "Delta tables"
    Delta tracks partitions automatically via the transaction log.
    `ADD PARTITION`, `DROP PARTITION`, and `MSCK REPAIR TABLE` are **not supported** on Delta tables.
    Use `INSERT`, `DELETE`, or `MERGE` to add or remove data, and `ALTER TABLE DROP PARTITION`
    is replaced by `DELETE FROM table WHERE part_col = 'val'`.

---

## :material-flask-outline: Practical Examples

### Add a partition for an external table

```sql
-- Files already exist at the path; just register in the metastore
ALTER TABLE events ADD IF NOT EXISTS
    PARTITION (event_date = '2024-06-01')
    LOCATION 's3://data-lake/events/event_date=2024-06-01/';
```

### Add multiple partitions in one statement

```sql
ALTER TABLE events ADD IF NOT EXISTS
    PARTITION (event_date = '2024-06-02') LOCATION 's3://data-lake/events/event_date=2024-06-02/'
    PARTITION (event_date = '2024-06-03') LOCATION 's3://data-lake/events/event_date=2024-06-03/';
```

### Drop a stale partition (managed table — data deleted)

```sql
ALTER TABLE sales DROP IF EXISTS
    PARTITION (region = 'APAC', order_date = '2023-01-01');
```

### Drop and purge (skip Trash)

```sql
ALTER TABLE sales DROP IF EXISTS
    PARTITION (region = 'EU', order_date = '2023-01-01')
    PURGE;
```

### Rename a partition key value

```sql
-- Rename region code without rewriting data
ALTER TABLE sales
    PARTITION (region = 'EMEA')
    RENAME TO PARTITION (region = 'EU');
```

### Repair unregistered partitions

```sql
-- After copying files manually into the table's S3 prefix
MSCK REPAIR TABLE events;

-- Verify
SHOW PARTITIONS events;
```

### Redirect a partition to a new path

```sql
ALTER TABLE archive_sales
    PARTITION (order_date = '2023-12-31')
    SET LOCATION 's3://cold-storage/sales/order_date=2023-12-31/';
```

---

## :material-lightbulb-outline: When to Use

| Scenario | Recommended Command |
|----------|---------------------|
| Files landed externally; need Spark to see them | `MSCK REPAIR TABLE` (non-Delta) |
| External table — register a single new partition | `ADD PARTITION ... LOCATION ...` |
| Remove historical data from a managed table | `DROP PARTITION [PURGE]` |
| Correct a wrong partition key value | `RENAME TO PARTITION` |
| Move a partition to cheaper/archival storage | `SET LOCATION` |
| Delta table — remove a date's worth of data | `DELETE FROM table WHERE part_col = 'val'` |

!!! tip "Batch ADD for backfill"
    When backfilling many partitions for an external table, build a single
    `ALTER TABLE ... ADD PARTITION` statement with all partitions rather than issuing
    one statement per partition — this reduces metastore round-trips significantly.
