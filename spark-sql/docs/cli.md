# :material-console: Spark sql cli

Spark SQL CLI is a command-line interface that allows you to run Spark SQL queries interactively or execute SQL scripts against a Spark cluster.

## :material-console: What is Spark SQL CLI?

A shell environment where you can type SQL queries directly.

It connects to a Spark session and executes SQL on your data.

Useful for quick ad-hoc queries, testing, or scripting without writing full Spark applications.

## :material-console: How to start Spark SQL CLI

From your terminal, run:

```bash
spark-sql
```

This opens an interactive prompt:

```sql
Spark SQL>
```

## :material-console: Binary path

/bin/spark-sql

## :material-console: Showing query output header

```bash
spark-sql  --conf "spark.hadoop.hive.cli.print.header=true"
```

## :material-console: Common features

1. Run SQL queries against Hive, Parquet, JSON, CSV, or Delta tables.
2. Support for Spark SQL syntax including DDL, DML, CTEs, joins, window functions, etc.
3. Load external data sources via CREATE TABLE USING commands.
4. Query tables registered in the Spark catalog.
5. Use CLI options to run SQL scripts or connect to a specific Hive metastore.

## :material-console: Running SQL scripts

You can run a .sql file non-interactively:

```bash
spark-sql -f /path/to/script.sql
```

## :material-console: CLI options highlights

1. --master <url>: Specify Spark master (e.g., local, yarn, mesos)
2. --conf <key>=<value>: Set Spark configs
3. -e <sql>: Run SQL statement and exit

Example:

```bash
spark-sql -e "SHOW TABLES"
```

## :material-console: Notes

1. spark-sql uses your Spark environment; make sure Spark is installed and environment variables are set.
2. CLI reads configs from $SPARK_HOME/conf and your Hadoop/Hive configs if available.
3. It's great for quick SQL access without needing to write Scala/Python code.
