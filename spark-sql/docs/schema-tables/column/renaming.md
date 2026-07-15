# :material-table-edit: Renaming & Altering Columns

`ALTER TABLE` commands rename, retype, reorder, and drop columns from an existing
table without rewriting data (for Delta tables). For non-Delta tables, most schema
changes require a full table rewrite.

---

## :material-code-tags: Syntax

```sql
-- Rename a column
ALTER TABLE table_name RENAME COLUMN old_name TO new_name;

-- Change column comment
ALTER TABLE table_name ALTER COLUMN col_name COMMENT 'new description';

-- Change column type (Delta — may require rewrite for incompatible types)
ALTER TABLE table_name ALTER COLUMN col_name TYPE new_type;

-- Set / drop NOT NULL
ALTER TABLE table_name ALTER COLUMN col_name SET NOT NULL;
ALTER TABLE table_name ALTER COLUMN col_name DROP NOT NULL;

-- Set / drop DEFAULT
ALTER TABLE table_name ALTER COLUMN col_name SET DEFAULT expression;
ALTER TABLE table_name ALTER COLUMN col_name DROP DEFAULT;

-- Drop a column
ALTER TABLE table_name DROP COLUMN col_name;
ALTER TABLE table_name DROP COLUMNS (col1, col2, col3);

-- Add a new column
ALTER TABLE table_name ADD COLUMN col_name data_type [NOT NULL] [DEFAULT expr] [COMMENT '...'];

-- Add multiple columns
ALTER TABLE table_name ADD COLUMNS (
    col1 data_type,
    col2 data_type COMMENT 'description'
);

-- Reorder a column (Delta / Databricks)
ALTER TABLE table_name ALTER COLUMN col_name AFTER other_col;
ALTER TABLE table_name ALTER COLUMN col_name FIRST;
```

---

## :material-information-outline: Behavior

1. **Delta Lake** supports column rename, drop, and type widening **without data rewrite** — changes are recorded in the transaction log; Parquet files on disk are unchanged.
2. **Type widening** (e.g., `INT → BIGINT`, `FLOAT → DOUBLE`) is safe in Delta; **narrowing** (`BIGINT → INT`) requires a full table rewrite and may lose data.
3. Dropping a column from a Delta table marks it as dropped in the log — old files still contain the column data. `VACUUM` eventually removes old files.
4. `RENAME COLUMN` requires `delta.columnMapping.mode = 'name'` (enabled by default in Databricks Runtime 10.2+).
5. Adding a column always appends it to the end of the schema unless `AFTER` / `FIRST` is used.
6. For **non-Delta** Parquet/ORC tables, most `ALTER COLUMN` changes (rename, drop, reorder) require recreating the table.

---

## :material-flask-outline: Practical Examples

### Rename a column

```sql
-- Rename for clarity or to fix a typo
ALTER TABLE customers RENAME COLUMN cust_nm TO customer_name;
ALTER TABLE orders    RENAME COLUMN amt     TO amount;
```

### Add a comment to a column

```sql
ALTER TABLE products ALTER COLUMN sku COMMENT 'Stock Keeping Unit — unique product identifier';
ALTER TABLE orders   ALTER COLUMN amount COMMENT 'Order total in USD';
```

### Change column type (widening — safe)

```sql
-- INT → BIGINT: safe widening, no data loss
ALTER TABLE events ALTER COLUMN event_id TYPE BIGINT;

-- FLOAT → DOUBLE: safe widening
ALTER TABLE metrics ALTER COLUMN score TYPE DOUBLE;
```

### Enforce NOT NULL on a column

```sql
-- Fails if any existing row has NULL in that column
ALTER TABLE customers ALTER COLUMN customer_id SET NOT NULL;
ALTER TABLE orders    ALTER COLUMN amount       SET NOT NULL;
```

### Drop NOT NULL (allow nulls)

```sql
ALTER TABLE orders ALTER COLUMN notes DROP NOT NULL;
```

### Add new columns

```sql
ALTER TABLE customers ADD COLUMNS (
    loyalty_points INT     COMMENT 'Accumulated loyalty points',
    tier           STRING  DEFAULT 'Bronze' COMMENT 'Customer tier: Bronze/Silver/Gold/Platinum'
);
```

### Drop a column

```sql
-- Drop a single column
ALTER TABLE orders DROP COLUMN internal_flag;

-- Drop multiple columns at once
ALTER TABLE events DROP COLUMNS (_ingested_at, _source_file, _batch_id);
```

### Reorder columns

```sql
-- Move customer_id to the first position
ALTER TABLE orders ALTER COLUMN customer_id FIRST;

-- Move notes after status
ALTER TABLE orders ALTER COLUMN notes AFTER status;
```

### Enable column mapping (required for RENAME on older Delta)

```sql
-- Enable column mapping for an existing Delta table
ALTER TABLE customers SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion'   = '2',
    'delta.minWriterVersion'   = '5'
);

-- Now rename is available
ALTER TABLE customers RENAME COLUMN cust_nm TO customer_name;
```

### Verify schema after changes

```sql
DESCRIBE TABLE customers;
DESCRIBE TABLE EXTENDED customers;   -- includes comments, defaults, constraints
```

---

## :material-swap-horizontal: Delta vs Non-Delta Schema Changes

| Operation | Delta | Parquet/ORC |
|-----------|-------|------------|
| `ADD COLUMN` | Metadata only — instant | Metadata only (Hive metastore) |
| `RENAME COLUMN` | Metadata only (column mapping required) | Requires full rewrite |
| `DROP COLUMN` | Metadata only — files unchanged until VACUUM | Requires full rewrite |
| `ALTER TYPE` (widening) | Metadata only | Requires full rewrite |
| `ALTER TYPE` (narrowing) | Full rewrite required | Full rewrite required |
| `REORDER COLUMN` | Metadata only | Requires full rewrite |

---

## :material-lightbulb-outline: When to Use

| Scenario | Command |
|----------|---------|
| Fix a column name typo | `RENAME COLUMN` |
| Add descriptive metadata | `ALTER COLUMN ... COMMENT` |
| Increase integer range | `ALTER COLUMN ... TYPE BIGINT` |
| Add a new business attribute | `ADD COLUMN` |
| Remove obsolete / sensitive column | `DROP COLUMN` |
| Promote a column to the front | `ALTER COLUMN ... FIRST` |
| Enforce data quality | `ALTER COLUMN ... SET NOT NULL` |

!!! tip "Run VACUUM after dropping columns"
    `DROP COLUMN` on a Delta table only marks the column as dropped in the transaction
    log — the underlying Parquet files still contain the column data.
    Run `VACUUM table_name RETAIN 0 HOURS` (after disabling retention check) to
    physically remove the old files and reclaim storage. In production, respect the
    default 7-day retention window.
