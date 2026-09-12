# :material-console: Spark SQL CLI

The **Spark SQL CLI** (`spark-sql`) is the command-line entry point for running SQL directly on a Spark driver without writing Python, Scala, or Java. It is best for ad-hoc exploration, smoke tests, and batch `.sql` execution against the same catalog and data sources that a regular Spark application can read.

In practice, this section covers three adjacent interfaces: local `spark-sql`, remote **Beeline** against a Spark Thrift Server, and `spark.sql()` embedded inside application code. They all run SQL, but they differ in where the session lives and how users connect to it.

### :material-animation-play: Interactive Visualization — Choosing the SQL Entry Point

Toggle between `spark-sql`, Beeline, and `spark.sql()` to see where the session runs, how the client connects, and which workflow each interface fits best.

<div id="viz-cli-overview" class="ts-viz"></div>

<script src="../../assets/js/configuration-cli-viz.js"></script>

______________________________________________________________________

## :material-sitemap: Architecture

```mermaid
flowchart LR
    U[User terminal] -->|spark-sql| CLI[spark-sql shell]
    CLI -->|starts local or cluster driver| SE[Spark SQL engine]
    APP[Python or Scala app] -->|spark.sql()| SE
    BL[Beeline JDBC client] -->|JDBC or Thrift| STS[Spark Thrift Server]
    STS -->|SparkSession| SE
    SE --> HM[Hive Metastore or catalog]
    SE --> FS[File system or object store]
    SE --> TAB[Managed or external tables]
```

______________________________________________________________________

## :material-compare: SQL Entry Points at a Glance

| Interface            | Session lives in            | Connection model         | Best for                                     |
| -------------------- | --------------------------- | ------------------------ | -------------------------------------------- |
| `spark-sql`          | Spark driver started by CLI | Local shell process      | Interactive exploration and running `.sql`   |
| `beeline`            | Spark Thrift Server         | JDBC / Thrift            | Shared remote access and BI-style tooling    |
| `spark.sql()`        | Your Spark application      | In-process API call      | Mixing SQL with Python, Scala, or Java logic |
| SQL Warehouse client | External service            | Vendor-specific protocol | Managed platform SQL endpoints               |

!!! note "Different network boundary"

    `spark-sql` starts a Spark application for you. Beeline does not: it only connects to an already-running Thrift Server, which is why `spark-sql` cannot be used as a JDBC client replacement.

______________________________________________________________________

## :material-rocket-launch: Quick Start

```bash
# Start an interactive shell
spark-sql

# Run one statement and exit
spark-sql -e "SHOW TABLES IN default"

# Run a script file and exit
spark-sql -f ./sql/daily_checks.sql

# Preload session state before the prompt appears
spark-sql --database default -i ./sql/session_init.sql

# Connect remotely to a Spark Thrift Server instead
beeline -u "jdbc:hive2://thrift-host:10000/default"
```

______________________________________________________________________

## :material-magnify: Behavior Highlights

1. **Interactive `spark-sql` owns its SparkSession** — temporary views, `SET` changes, and loaded functions disappear when the process exits.
2. **Beeline is remote-first** — the SQL session lives on the Thrift Server, so connection strings, authentication, and session isolation matter more than local Spark installation details.
3. **`spark.sql()` shares application state** — SQL statements can read temp views, configs, and DataFrames created earlier in the same application.
4. **The same SQL text can move between interfaces** — the choice usually depends more on session ownership and automation style than on SQL syntax.

______________________________________________________________________

## :material-book-open-variant: In This Section

| Page                                  | Contents                                                      |
| ------------------------------------- | ------------------------------------------------------------- |
| [Getting Started](getting-started.md) | First launch, prompt behavior, file queries, and shell basics |
| [Options Reference](options.md)       | CLI-specific flags plus the inherited Spark launcher options  |
| [Scripting](scripting.md)             | `.sql` files, stdin, variables, `source`, and batch patterns  |
| [Configuration](configuration.md)     | Precedence, `SET`, Spark defaults, and Hive-related config    |
| [Beeline](beeline.md)                 | JDBC URLs, Thrift Server setup, remote sessions, and exports  |
