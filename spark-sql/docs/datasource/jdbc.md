# :material-database-outline: JDBC Data Source

The Spark SQL JDBC connector reads from and writes to relational databases —
PostgreSQL, MySQL, SQL Server, Oracle, and any JDBC-compliant database.
Use it for **ingestion** and **federation queries**, not as a primary analytics store.

---

## :material-pin: Options Reference

| Option | Required | Description |
|--------|:--------:|-------------|
| `url` | Yes | JDBC connection URL, e.g. `jdbc:postgresql://host:5432/db` |
| `dbtable` | Yes* | Table or subquery: `schema.table` or `(SELECT …) AS alias` |
| `query` | Yes* | SQL query string (alternative to `dbtable`) |
| `user` | No | Database username |
| `password` | No | Database password (use Databricks Secrets) |
| `driver` | No | JDBC driver class, e.g. `org.postgresql.Driver` |
| `numPartitions` | No | Number of Spark partitions for parallel reads |
| `partitionColumn` | No | Numeric/date column for parallel partitioning |
| `lowerBound` | No | Lower bound for `partitionColumn` |
| `upperBound` | No | Upper bound for `partitionColumn` |
| `fetchsize` | `0` | Rows to fetch per round-trip (tune for large reads) |
| `batchsize` | `1000` | Rows per INSERT batch (write performance) |
| `isolationLevel` | `READ_UNCOMMITTED` | Transaction isolation: `NONE`, `READ_COMMITTED`, etc. |
| `queryTimeout` | `0` | Query timeout in seconds |
| `pushDownPredicate` | `true` | Push WHERE clauses to the database |
| `pushDownAggregate` | `false` | Push GROUP BY to the database |
| `truncate` | `false` | TRUNCATE instead of DROP+CREATE on overwrite |

*Either `dbtable` or `query` is required.

---

## :material-flask-outline: Examples

### Read entire table

```sql
CREATE OR REPLACE TEMP VIEW pg_customers
USING jdbc
OPTIONS (
    url      = 'jdbc:postgresql://db-host:5432/mydb',
    dbtable  = 'public.customers',
    user     = 'spark_reader',
    password = secret('scope', 'db_password'),
    driver   = 'org.postgresql.Driver'
);

SELECT * FROM pg_customers LIMIT 10;
```

### Read with a subquery (push filter to DB)

```sql
CREATE OR REPLACE TEMP VIEW recent_orders
USING jdbc
OPTIONS (
    url     = 'jdbc:postgresql://db-host:5432/orders_db',
    dbtable = "(SELECT order_id, customer_id, amount, order_date
                FROM orders
                WHERE order_date >= CURRENT_DATE - INTERVAL '7 days') AS recent",
    user    = 'spark_reader',
    password = secret('scope', 'db_password')
);
```

### Parallel read with partitionColumn

```sql
-- Splits the table into 8 partitions by order_id range
CREATE OR REPLACE TEMP VIEW orders_parallel
USING jdbc
OPTIONS (
    url             = 'jdbc:postgresql://db-host:5432/orders_db',
    dbtable         = 'public.orders',
    user            = 'spark_reader',
    password        = secret('scope', 'db_password'),
    partitionColumn = 'order_id',
    lowerBound      = '1',
    upperBound      = '10000000',
    numPartitions   = '8',
    fetchsize       = '10000'
);
```

### Write to a database table (append)

```sql
-- CTAS writes Spark result to PostgreSQL
CREATE TABLE jdbc.pg_summary
USING jdbc
OPTIONS (
    url       = 'jdbc:postgresql://db-host:5432/reports',
    dbtable   = 'public.daily_summary',
    user      = 'spark_writer',
    password  = secret('scope', 'db_password'),
    batchsize = '5000'
)
AS
SELECT order_date, region, COUNT(*) AS orders, SUM(amount) AS revenue
FROM analytics.orders
GROUP BY order_date, region;
```

### Write with overwrite (truncate)

```sql
INSERT OVERWRITE TABLE jdbc.pg_summary
SELECT order_date, region, COUNT(*) AS orders, SUM(amount) AS revenue
FROM analytics.orders
WHERE order_date = current_date()
GROUP BY order_date, region;
```

### Use Databricks Secrets for credentials

```sql
CREATE OR REPLACE TEMP VIEW secure_view
USING jdbc
OPTIONS (
    url      = 'jdbc:sqlserver://sql-host:1433;databaseName=mydb',
    dbtable  = 'dbo.employees',
    user     = secret('db-scope', 'sql_user'),
    password = secret('db-scope', 'sql_password'),
    driver   = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
);
```

### MySQL example

```sql
CREATE OR REPLACE TEMP VIEW mysql_products
USING jdbc
OPTIONS (
    url      = 'jdbc:mysql://mysql-host:3306/inventory?useSSL=true',
    dbtable  = 'products',
    user     = 'reader',
    password = secret('mysql-scope', 'password'),
    driver   = 'com.mysql.cj.jdbc.Driver',
    fetchsize = '2000'
);
```

---

## :material-speedometer: Performance Tips

| Tip | Reason |
|-----|--------|
| Use `partitionColumn` + `numPartitions` | Parallel reads instead of a single-thread scan |
| Push filters via `dbtable` subquery | Reduces rows transferred from DB to Spark |
| Set `fetchsize = 10000` | Fewer round-trips for large reads |
| Set `batchsize = 5000` | Fewer round-trips for bulk writes |
| Enable `pushDownPredicate = 'true'` (default) | WHERE pushdown saves network I/O |
| Avoid reading during peak DB hours | JDBC reads add load to the source DB |
| Write to a staging table, then swap | Avoids locking production tables during writes |

!!! warning "Never use JDBC as your analytics store"
    JDBC reads are row-oriented and serialized per partition.
    For analytics queries, land data into Delta/Parquet first and query from there.

---

## :material-alert-circle: Common Mistakes

| Mistake | Problem | Fix |
|---------|---------|-----|
| Hard-coding credentials in OPTIONS | Secret leak in query history | Use `secret('scope', 'key')` |
| No `numPartitions` on large tables | Single-threaded read — very slow | Set `partitionColumn` + bounds + `numPartitions` |
| `upperBound` below actual max | Some rows are never read | Use `(SELECT MAX(id) FROM table)` to set dynamically |
| Writing without `truncate = 'true'` on overwrite | Drops and recreates target table (loses indexes) | Set `truncate = 'true'` to truncate instead |
| Reading a large table with `fetchsize = 0` | OOM on driver — default is unlimited | Set `fetchsize = 5000` or higher |

---

## :material-brain: When to Use JDBC

| Scenario | Recommendation |
|----------|----------------|
| Initial ingestion from RDBMS | JDBC read → write to Delta |
| Near-real-time data from operational DB | JDBC with `dbtable` subquery + schedule |
| Writing aggregated results back to DB | JDBC write with `batchsize` |
| Production analytics queries | Land to Delta first; never query DB directly |
| Schema discovery | JDBC read + `DESCRIBE` / `SHOW COLUMNS` |
