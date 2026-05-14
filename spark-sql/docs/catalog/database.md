# :material-database: Databases (Schemas)

In Spark SQL, **DATABASE** and **SCHEMA** are synonyms. A database is a logical
namespace that groups tables, views, and functions within a catalog.

---

## :material-console: DDL Reference

### CREATE DATABASE

```sql
CREATE DATABASE [IF NOT EXISTS] db_name
  [COMMENT 'description']
  [LOCATION '/path/to/warehouse/dir']
  [WITH DBPROPERTIES (key = 'value', ...)];
```

```sql
-- Minimal
CREATE DATABASE analytics;

-- With location and comment
CREATE DATABASE IF NOT EXISTS analytics
  COMMENT 'Analytics domain — orders and revenue'
  LOCATION 'dbfs:/mnt/analytics/warehouse'
  WITH DBPROPERTIES (owner = 'data-eng', env = 'prod');
```

### ALTER DATABASE

```sql
-- Update properties
ALTER DATABASE analytics SET DBPROPERTIES (owner = 'analytics-team');

-- Change the warehouse location (Databricks / Spark 3.4+)
ALTER DATABASE analytics SET LOCATION 'dbfs:/mnt/v2/analytics';
```

### DROP DATABASE

```sql
-- Safe drop — fails if database contains objects
DROP DATABASE IF EXISTS analytics;

-- Cascade — drops all tables and views inside first
DROP DATABASE IF EXISTS analytics CASCADE;
```

!!! warning "CASCADE deletes managed table data"
    `DROP DATABASE CASCADE` deletes metadata **and** the underlying data of
    any managed tables inside the database. External table data is not deleted.

### USE — Set the Active Database

```sql
USE analytics;

-- Verify
SELECT current_database();  -- analytics
```

---

## :material-magnify: Inspect a Database

```sql
-- List all databases in the current catalog
SHOW DATABASES;

-- Filter by name pattern
SHOW DATABASES LIKE 'anal*';

-- Detailed metadata: comment, location, owner
DESCRIBE DATABASE EXTENDED analytics;
-- Output includes:
-- Database Name  analytics
-- Comment        Analytics domain...
-- Location       dbfs:/mnt/analytics/warehouse
-- Owner          spark
-- Properties     ((owner,data-eng), (env,prod))
```

---

## :material-folder-open: Database Location and Warehouse Directory

By default Spark stores managed table data under:
```
spark.sql.warehouse.dir / <database_name>.db / <table_name>
```

Specifying `LOCATION` overrides this for the entire database:

```sql
CREATE DATABASE silver
  LOCATION 'dbfs:/mnt/lakehouse/silver';
-- All managed tables in 'silver' will be stored under that path
```

---

## :material-flask-outline: Practical Patterns

### Multi-Environment Schema Isolation

```sql
-- Separate schemas per environment in one catalog
CREATE DATABASE IF NOT EXISTS orders_dev;
CREATE DATABASE IF NOT EXISTS orders_test;
CREATE DATABASE IF NOT EXISTS orders_prod;

-- Create identical table structures
CREATE TABLE orders_dev.sales  LIKE orders_prod.sales;
```

### Namespace a Domain

```sql
CREATE DATABASE IF NOT EXISTS finance  COMMENT 'Finance domain';
CREATE DATABASE IF NOT EXISTS marketing COMMENT 'Marketing domain';
CREATE DATABASE IF NOT EXISTS ops       COMMENT 'Ops / SRE domain';
```

### Show Tables in Another Database (without USE)

```sql
SHOW TABLES IN analytics;

-- Fully-qualified cross-database query
SELECT * FROM analytics.orders WHERE order_date = '2024-06-01';
```

---

## :material-magnify: Behavior Notes

1. **DATABASE ≡ SCHEMA** — both keywords are interchangeable in all DDL.
2. **USE is session-scoped** — `USE db` only affects the current session.
3. **Default database** — new sessions start in the `default` database.
4. **Managed table cleanup** — dropping a managed table inside a database removes its data from the database location.
5. **External tables survive DROP DATABASE** — only metadata is deleted; the physical files remain.

---

## :material-brain: When to Use

| Scenario | Recommendation |
|----------|----------------|
| Separate business domains | One database per domain |
| Multi-environment isolation | `orders_dev`, `orders_test`, `orders_prod` |
| Shared warehouse path | Specify `LOCATION` explicitly |
| Scratch / ad-hoc work | `default` or a personal dev database |
