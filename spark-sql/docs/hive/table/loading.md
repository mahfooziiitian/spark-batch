# :material-bee: Loading Hive Tables

Hive tables can be loaded from files or inserted using Spark SQL.

### :material-sitemap: Overview

```mermaid
graph LR
    A["Hive Metastore"] --> B["Spark SQL Catalog"]
    B --> C["Tables / Partitions / Views"]
    C --> D["Query Execution"]
```

---

## :material-pin: Common Methods

| Method | Example |
|--------|---------|
| `LOAD DATA` | Load files into table location |
| `INSERT INTO` | Append rows from a query |
| `INSERT OVERWRITE` | Replace data |

---

## :material-flask-outline: Examples

```sql
LOAD DATA INPATH 's3://data/sales/' INTO TABLE hive_sales;
```

```sql
INSERT INTO hive_sales
SELECT * FROM staging_sales;
```

---

## :material-brain: When to Use

| Scenario | Recommendation |
|----------|----------------|
| Bulk load from files | `LOAD DATA` |
| Transform and load | `INSERT INTO ... SELECT` |
| Replace partitions | `INSERT OVERWRITE` |
