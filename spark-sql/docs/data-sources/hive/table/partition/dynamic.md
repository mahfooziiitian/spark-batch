# :material-bee: Dynamic Partition Insert

**Dynamic partitioning** lets Spark derive each row's target partition from the data
itself, instead of naming the partition explicitly. It is the standard way to write many
partitions in a single `INSERT`.

### :material-sitemap: Static vs Dynamic

```mermaid
graph LR
    A["INSERT ... SELECT"] --> B{"Partition value\nspecified?"}
    B -->|Yes: PARTITION(state='CA')| C["Static — one partition"]
    B -->|No: PARTITION(state)| D["Dynamic — many partitions from data"]
```

---

## :material-pin: Syntax

```sql
-- Dynamic: partition column comes last in the SELECT
INSERT INTO TABLE sales
PARTITION (state)
SELECT id, amount, state
FROM staging_sales;
```

The partition column(s) must be the **trailing** columns of the `SELECT` list, in
`PARTITION (...)` order.

---

## :material-magnify: Behavior

1. Spark creates one subdirectory per distinct partition value found in the data.
2. `SHOW PARTITIONS` then lists the created partitions (verified: `state=CA`, `state=NY`).
3. Overwrite scope is controlled by `spark.sql.sources.partitionOverwriteMode`:

    | Mode | `INSERT OVERWRITE` effect |
    |------|---------------------------|
    | `STATIC` (default) | Replaces **all** partitions of the table |
    | `DYNAMIC` | Replaces **only** the partitions present in the incoming data |

4. On classic Hive-managed tables, `hive.exec.dynamic.partition=true` and
   `hive.exec.dynamic.partition.mode=nonstrict` may also be required.

```sql
SET spark.sql.sources.partitionOverwriteMode = DYNAMIC;

INSERT OVERWRITE TABLE sales
PARTITION (state)
SELECT id, amount, state FROM staging_sales;   -- only touched states replaced
```

---

## :material-alert: Pitfalls

- **Column order** — a mismatch silently routes values to the wrong partition.
- **Too many partitions** — high-cardinality dynamic columns create many small files.
- **STATIC overwrite surprise** — leaving the default `STATIC` mode wipes untouched
  partitions on `INSERT OVERWRITE`.

---

## :material-brain: When to Use

| Scenario | Recommendation |
|----------|----------------|
| Partition value lives in the data | Dynamic partitions |
| Writing many partitions at once | Dynamic partitions |
| Replacing only affected partitions | `DYNAMIC` overwrite mode |
| Single, known target partition | Static `PARTITION (col = 'value')` |

!!! warning "Guard INSERT OVERWRITE"
    With the default `STATIC` mode, `INSERT OVERWRITE` on a partitioned table replaces
    **every** partition. Set `partitionOverwriteMode=DYNAMIC` for partial refreshes — see
    [Partition DDL](ddl.md) and [Loading Hive Tables](../loading.md).
