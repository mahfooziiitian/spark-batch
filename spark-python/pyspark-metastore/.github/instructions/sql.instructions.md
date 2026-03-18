---
applyTo: "{**/*.sql,src/**/*.py}"
---

# Spark SQL — Metastore & Catalog Operations

## SQL Style

- Keywords in **UPPERCASE**: `SELECT`, `CREATE TABLE`, `SHOW DATABASES`.
- Identifiers in **lowercase**: `my_catalog.my_db.my_table`.
- Use the SQLFluff Databricks dialect configured in `pyproject.toml`.

## Catalog DDL

### Listing metadata

```sql
SHOW CATALOGS;
SHOW DATABASES IN spark_catalog;
SHOW TABLES IN spark_catalog.default;
DESCRIBE TABLE spark_catalog.default.my_table;
DESCRIBE TABLE EXTENDED spark_catalog.default.my_table;
```

### Switching context

```sql
USE CATALOG my_catalog;
USE my_database;
SET spark.sql.defaultCatalog = my_catalog;
```

### Creating databases and tables

```sql
CREATE DATABASE IF NOT EXISTS my_catalog.analytics;

CREATE TABLE IF NOT EXISTS my_catalog.analytics.events (
    id        INT,
    event     STRING,
    ts        TIMESTAMP
) USING PARQUET;
```

### Managed vs external tables

```sql
-- Managed table (data in warehouse dir)
CREATE TABLE my_catalog.default.managed_tbl (id INT, name STRING);

-- External table (data at explicit location)
CREATE TABLE my_catalog.default.external_tbl (id INT, name STRING)
USING PARQUET
LOCATION 's3://bucket/path/to/data';
```

### Drop with safety

```sql
DROP TABLE IF EXISTS my_catalog.default.my_table;
DROP DATABASE IF EXISTS my_catalog.analytics CASCADE;
```

## Three-Level Namespace

Spark 3+ uses `catalog.database.table`. Always demonstrate all three levels:

```sql
-- Fully qualified
SELECT * FROM spark_catalog.default.my_table;

-- Default catalog, explicit database
SELECT * FROM default.my_table;

-- Current catalog + current database
SELECT * FROM my_table;
```

## Inline SQL in Python

Use triple-quoted strings for multi-line SQL and `spark.sql()`:

```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS spark_catalog.default.employees (
        id   INT,
        name STRING
    )
""")

spark.sql("INSERT INTO spark_catalog.default.employees VALUES (1, 'Alice'), (2, 'Bob')")

df = spark.sql("SELECT * FROM spark_catalog.default.employees")
df.show()
```

For dynamic table names use f-strings — but **never** interpolate untrusted user input:

```python
def drop_table_if_exists(spark, table_name: str):
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
```

## Catalog-Specific SQL

### Iceberg

```sql
SELECT * FROM my_iceberg.db.events VERSION AS OF 123456789;
SELECT * FROM my_iceberg.db.events.snapshots;
CALL my_iceberg.system.expire_snapshots('db.events', TIMESTAMP '2024-01-01 00:00:00');
```

### Delta Lake

```sql
DESCRIBE HISTORY spark_catalog.default.delta_table;
SELECT * FROM spark_catalog.default.delta_table VERSION AS OF 5;
VACUUM spark_catalog.default.delta_table RETAIN 168 HOURS;
```

### Hive

```sql
CREATE TABLE IF NOT EXISTS hive.analytics.logs (
    message STRING
) PARTITIONED BY (dt STRING)
STORED AS PARQUET;

MSCK REPAIR TABLE hive.analytics.logs;
```

## SQLFluff Config

Key settings from `pyproject.toml`:

```toml
[tool.sqlfluff.core]
dialect = "databricks"
sql_file_exts = ".sql,.sql.j2,.dml,.ddl"

[tool.sqlfluff.rules.capitalisation.keywords]
capitalisation_policy = "upper"
```

Run with:
```bash
uv run task sql_format   # sqlfluff fix
uv run task sql_lint     # sqlfluff lint
```
