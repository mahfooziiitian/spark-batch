# Copilot Instructions — pyspark-ds-jdbc

## Project Overview

This project demonstrates **JDBC database connectivity with Apache Spark**, covering reading from and writing to **Oracle**, **MySQL**, and **MSSQL** databases using PySpark's JDBC datasource API.

## Tech Stack

- **Language:** Python ≥ 3.11
- **Framework:** PySpark ^3.5.1
- **Package Manager:** Poetry (build backend: `poetry-core`)
- **Testing:** pytest ^8.2.2
- **JDBC Drivers:**
  - Oracle: `com.oracle.database.jdbc:ojdbc11:23.2.0.0` / `ojdbc8:23.5.0.24.07`
  - MySQL: `com.mysql:mysql-connector-j:8.0.33`

## Project Structure

```
src/
├── connection/          # JDBC connection provider documentation
├── reader/
│   ├── jdbc_reader.py   # Main reader: spark.read.jdbc() with Window dedup
│   ├── queries/cte/     # CTE-based prepared queries (MySQL, Oracle, MSSQL)
│   ├── predicates/      # Predicate pushdown, aggregate pushdown
│   ├── data_type/       # CLOB, JSON column handling (Oracle)
│   ├── tables/          # dbtable-based reading
│   └── options/         # MySQL-specific reader options
├── writer/
│   ├── jdbc_writer.py   # Write DataFrames to MySQL via JDBC
│   └── level/           # Transaction isolation levels
├── utils/
│   ├── spark_util.py    # get_spark_session(configs: Dict) helper
│   └── config_reader.py # ConfigReader for DB credentials (user, password, driver, url)
├── ddl/                 # DDL operations (createTableOptions, createTableColumnTypes)
├── schema/              # Custom schema management (customSchema option)
└── partition/           # Partition strategies for parallel reads/writes
tests/
└── reader/              # pytest tests for JDBC readers
```

## Modular Instruction Files

| File | Scope | Purpose |
|------|-------|---------|
| `instructions/python.instructions.md` | `**/*.py` | Python coding style and conventions |
| `instructions/pyspark-jdbc.instructions.md` | `src/**/*.py` | JDBC read/write patterns and Spark conventions |
| `instructions/testing.instructions.md` | `tests/**/*.py` | pytest conventions and test patterns |
| `instructions/project-config.instructions.md` | `pyproject.toml`, `poetry.lock` | Poetry package configuration |

## Quick Reference

- **SparkSession:** Always create via `get_spark_session(configs: Dict)` from `utils.spark_util`
- **DB Config:** Use `ConfigReader(config_path)` from `utils.config_reader` for credentials
- **Config Path:** `os.environ['DATA_HOME'] + "\\Database\\Config\\<DB>\\db.conf"`
- **JDBC Read:** `spark.read.jdbc(url, table, properties=props)` or builder-style with `.format("jdbc")`
- **JDBC Write:** `df.write.jdbc(url, table, mode, properties=props)`
- **Deduplication:** Window `rank()` over partition → filter `rank==1`
- **CTE Queries:** Use `prepareQuery` option for WITH clauses, paired with `query` option

## Things to Avoid

- Do not hardcode database credentials — always use `ConfigReader` with external config files
- Do not create SparkSession directly with `SparkSession.builder` — use `get_spark_session()` utility
- Do not specify both `dbtable` and `query` options simultaneously (Spark will raise an error)
- Do not use `query` with `partitionColumn` (they are mutually exclusive)
- Do not commit JDBC passwords, connection strings, or config files to version control
- Do not use `select *` in JDBC queries when only specific columns are needed — push column selection down
- Do not ignore `fetchsize` / `batchsize` tuning — defaults may cause poor performance
