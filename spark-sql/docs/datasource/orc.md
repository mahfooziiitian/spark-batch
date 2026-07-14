# :material-table: ORC Data Source

ORC (Optimized Row Columnar) is a columnar format native to the Hive ecosystem.
It offers excellent compression and predicate pushdown for Hive-originated workloads.
For new Spark / Databricks projects, prefer **Parquet** or **Delta** — ORC is best when
interoperating with existing Hive tables.

---

## :material-pin: Options Reference

| Option | Default | Description |
|--------|---------|-------------|
| `path` | — | File or directory path |
| `mergeSchema` | `false` | Union schemas across ORC files |
| `compression` | `snappy` | Codec: `none`, `snappy`, `zlib`, `lzo`, `zstd` |
| `orcFilterPushdown` | `true` | Enable predicate pushdown into ORC stripes |
| `recursiveFileLookup` | `false` | Search subdirectories |

---

## :material-flask-outline: Examples

### Create a managed ORC table

```sql
CREATE TABLE IF NOT EXISTS hive_compat.sales (
    sale_id    INT,
    product_id STRING,
    amount     DOUBLE,
    sale_date  DATE
)
USING orc
PARTITIONED BY (sale_date)
TBLPROPERTIES ('orc.compress' = 'SNAPPY');
```

### Read external ORC files

```sql
CREATE OR REPLACE TEMP VIEW orc_events
USING orc
OPTIONS (
    path        = 'hdfs://namenode/data/events/year=2024/',
    mergeSchema = 'false'
);

SELECT event_type, COUNT(*) FROM orc_events GROUP BY event_type;
```

### CTAS from ORC to Delta

```sql
-- Migrate a Hive ORC table to Delta for ACID support
CREATE TABLE analytics.sales_delta
USING delta
PARTITIONED BY (sale_date)
AS SELECT * FROM hive_compat.sales;
```

### Insert into ORC

```sql
INSERT INTO hive_compat.sales
SELECT sale_id, product_id, amount, sale_date
FROM staging.sales
WHERE sale_date = current_date();
```

### Configure compression

```sql
SET spark.sql.orc.compression.codec = 'zstd';

CREATE TABLE hive_compat.events_zstd
USING orc
AS SELECT * FROM staging.events;
```

---

## :material-compare: ORC vs Parquet vs Delta

| Feature | ORC | Parquet | Delta |
|---------|:---:|:-------:|:-----:|
| Columnar | :material-check: | :material-check: | :material-check: |
| Hive native | :material-check: | Supported | Supported |
| Spark native | Supported | :material-check: | :material-check: |
| ACID | Via Hive | :material-close: | :material-check: |
| Bloom filters | :material-check: | Via write option | :material-check: |
| Stripe/row-group metadata | Stripes | Row groups | Row groups |
| Best for | Hive workloads | Spark analytics | Spark + ACID |

---

## :material-brain: When to Use ORC

| Scenario | Recommendation |
|----------|----------------|
| Existing Hive ecosystem | ORC — native format |
| New Spark / Databricks project | Parquet or Delta |
| Hive ACID tables | ORC with Hive ACID |
| Migrating from Hive to Databricks | Convert ORC → Delta |
