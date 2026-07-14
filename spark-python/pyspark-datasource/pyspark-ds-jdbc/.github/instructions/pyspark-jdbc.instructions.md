---
applyTo: "src/**/*.py"
---

# PySpark JDBC Patterns & Conventions

## SparkSession Creation

- Always create the SparkSession via the `get_spark_session(configs: Dict)` utility in `utils.spark_util`.
- Pass Spark configurations as a dictionary:
  ```python
  from utils.spark_util import get_spark_session

  configs = {
      "spark.app.name": "MySparkApp",
      "spark.master": "local[*]",
      "spark.jars.packages": "com.oracle.database.jdbc:ojdbc11:23.2.0.0"
  }
  spark = get_spark_session(configs)
  ```
- For multiple JDBC JARs, comma-separate them in `spark.jars.packages`:
  ```python
  "spark.jars.packages": "com.oracle.database.jdbc:ojdbc11:23.2.0.0,com.mysql:mysql-connector-j:8.0.33"
  ```

## Database Configuration

- Use `ConfigReader(config_path)` from `utils.config_reader` to load DB credentials.
- Config path follows the pattern: `os.environ['DATA_HOME'] + "\\Database\\Config\\<DB>\\db.conf"`
  - `<DB>` is one of: `Oracle`, `MySQL`, `MSSQL`
- Available methods: `get_user()`, `get_password()`, `get_driver()`, `get_url()`, `get_host()`, `get_dns()`.
- Never hardcode credentials — always load from external config files.

## JDBC Read Patterns

### Basic Read with `spark.read.jdbc()`
```python
properties = {
    "user": config_reader.get_user(),
    "password": config_reader.get_password(),
    "driver": config_reader.get_driver(),
    "fetchsize": "1000"
}
df = spark.read.jdbc(url=jdbc_url, table=source_table, properties=properties)
```

### Builder-Style Read with `.format("jdbc")`
```python
df = (spark.read.format("jdbc")
      .option("url", config_reader.get_url())
      .option("dbtable", "table_name")
      .option("user", config_reader.get_user())
      .option("password", config_reader.get_password())
      .option("driver", config_reader.get_driver())
      .load())
```

### JDBC Read Options
- `fetchsize` — number of rows fetched per round trip (default varies by driver; set explicitly, e.g. `"1000"`)
- `numPartitions` — number of parallel partitions for the read
- `partitionColumn` — integer column used to split the read into parallel tasks
- `lowerBound` / `upperBound` — range for `partitionColumn` to determine partition strides
- `customSchema` — override inferred column types: `"id DECIMAL(38, 0), name STRING, age INT"`

### Query Option (Subquery Pushdown)
- Use the `query` option to push SQL down to the database:
  ```python
  df = spark.read.format("jdbc").option("query", "SELECT col1, col2 FROM table WHERE ...").load()
  ```
- **Do not** specify both `dbtable` and `query` simultaneously.
- **Do not** use `query` with `partitionColumn` (they are mutually exclusive).

### CTE / Prepared Queries
- Use the `prepareQuery` option for WITH (CTE) clauses, paired with `query`:
  ```python
  df = (spark.read.format("jdbc")
        .option("prepareQuery", "WITH t AS (SELECT col1, col2 FROM source_table)")
        .option("query", "SELECT * FROM t WHERE condition")
        .options(**reader_properties)
        .load())
  ```
- **MySQL**: standard CTE with `prepareQuery` + `query`.
- **Oracle**: CTE with `prepareQuery` + `query`; also supports JSON_TABLE in CTEs.
- **MSSQL**: use `prepareQuery` for temp table creation (`SELECT * INTO #TempTable FROM ...`), then `query` reads from the temp table.

### Predicate Pushdown
- Use the `predicates` parameter to push WHERE clauses to the database:
  ```python
  predicates = ["topic='movies' order by batch_id desc limit 1"]
  df = spark.read.jdbc(url=url, table=table, predicates=predicates, properties=props)
  ```
- Each predicate string creates one partition; Spark sends the predicate as a WHERE clause.

## Oracle-Specific Patterns

### CLOB Columns
- Read CLOB columns directly; Spark maps them to StringType:
  ```python
  query = "select * from table_with_clob"
  df = spark.read.format("jdbc").options(**reader_properties).load()
  ```

### JSON Columns
- Oracle JSON type requires `json_serialize()` or `json_value()` to extract as string:
  ```python
  query = "select json_serialize(details) from kafka_offset"
  ```
- For complex JSON, use `JSON_TABLE` in a CTE to flatten nested structures.

## MySQL-Specific Patterns

- Use `com.mysql:mysql-connector-j:<version>` as the JDBC driver package.
- Builder-style `.option()` chain is the preferred read pattern for MySQL.

## MSSQL-Specific Patterns

- Use `prepareQuery` to create temporary tables (`#TempTable`) for complex queries.
- The `query` option then reads from the temp table.

## JDBC Write Patterns

### Basic Write
```python
write_properties = {
    "user": config_reader.get_user(),
    "password": config_reader.get_password(),
    "driver": config_reader.get_driver(),
    "batchsize": "1000",
    "isolationLevel": "READ_COMMITTED"
}
df.write.jdbc(url=jdbc_url, table=dest_table, mode="overwrite", properties=write_properties)
```

### Builder-Style Write
```python
(df.write
 .option("user", config_reader.get_user())
 .option("driver", config_reader.get_driver())
 .option("password", config_reader.get_password())
 .jdbc(url=config_reader.get_url(), table="table_name", mode="overwrite"))
```

### Write Modes
- `overwrite` — drop and recreate the table
- `append` — insert rows into the existing table

### Write Options
- `batchsize` — number of rows per INSERT batch (e.g. `"1000"`)
- `isolationLevel` — transaction isolation level for the write

### Transaction Isolation Levels
- `NONE` — no transaction isolation
- `READ_UNCOMMITTED` — allows dirty reads
- `READ_COMMITTED` — prevents dirty reads (most common default)
- `REPEATABLE_READ` — prevents dirty and non-repeatable reads
- `SERIALIZABLE` — strictest; prevents phantom reads

## DDL Operations

### createTableOptions
- Pass DDL options for table creation:
  ```python
  df.write.option("createTableOptions", "ENGINE=InnoDB DEFAULT CHARSET=utf8").jdbc(...)
  ```

### createTableColumnTypes
- Override default column type mappings:
  ```python
  df.write.option("createTableColumnTypes", "id int, name varchar(100), age int").jdbc(...)
  ```
- Specified types must be valid Spark SQL data types.

## Window Function Deduplication

- Use `rank()` over a Window specification to deduplicate rows:
  ```python
  from pyspark.sql import functions as F, Window

  window_spec = Window.partitionBy("group_col").orderBy(df["sort_col"].desc())
  df = df.select("*", F.rank().over(window_spec).alias("rank")).where("rank == 1")
  ```
- Partition by the grouping key, order by the column that determines the "latest" or "best" row.

## Partition Strategies for Parallel Reads

- Use `numPartitions`, `partitionColumn`, `lowerBound`, and `upperBound` together for parallel reads.
- The stride per partition is: `(upperBound - lowerBound) / numPartitions`.
- `partitionColumn` must be a numeric, date, or timestamp column.
- For writes, use `.partitionBy("column")` to partition output by a column value.

## Schema Management

- Use `customSchema` to override inferred types when database types don't map well:
  ```python
  .option("customSchema", "id DECIMAL(38, 0), name STRING, age INT")
  ```
