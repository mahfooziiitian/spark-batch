# :material-format-list-bulleted: Options Reference

Complete reference for `spark-sql` command-line flags.

---

## :material-console: Syntax

```bash
spark-sql [options] [--conf key=value ...] [-f filename | -e sql]
```

---

## :material-table: Full Options Reference

| Flag | Argument | Description |
|------|----------|-------------|
| `--master` | `<url>` | Spark master URL: `local`, `local[N]`, `yarn`, `k8s://...` |
| `--deploy-mode` | `client\|cluster` | Driver placement (default: `client`) |
| `-e` | `<sql>` | Execute a SQL statement and exit |
| `-f` | `<file>` | Execute a SQL script file and exit |
| `-i` | `<file>` | Initialise session by running a SQL file before the prompt |
| `--database` | `<db>` | Set the active database on startup |
| `--conf` | `<key>=<value>` | Set a Spark configuration property |
| `--driver-memory` | `<mem>` | Driver JVM memory (e.g. `4g`) |
| `--driver-cores` | `<n>` | Driver cores (cluster mode only) |
| `--executor-memory` | `<mem>` | Executor JVM memory (e.g. `8g`) |
| `--executor-cores` | `<n>` | Cores per executor |
| `--num-executors` | `<n>` | Number of executors (YARN/K8s) |
| `--jars` | `<jars>` | Comma-separated extra JARs to add to the classpath |
| `--packages` | `<coords>` | Maven coordinates to resolve and add (e.g. Iceberg) |
| `--repositories` | `<urls>` | Extra Maven repositories for `--packages` |
| `--files` | `<files>` | Comma-separated files to upload to executors |
| `--py-files` | `<files>` | `.py` / `.zip` / `.egg` files for Python UDFs |
| `--name` | `<name>` | Application name (shown in Spark UI) |
| `--queue` | `<queue>` | YARN queue name |
| `--principal` | `<principal>` | Kerberos principal |
| `--keytab` | `<file>` | Kerberos keytab file |
| `--verbose` | — | Print extra debug information |
| `--version` | — | Print Spark version and exit |
| `--help` | — | Print usage and exit |

---

## :material-flask-outline: Common Invocation Examples

```bash
# Interactive shell — local, 8 cores, 4 GB driver
spark-sql \
  --master local[8] \
  --driver-memory 4g \
  --conf spark.sql.adaptive.enabled=true

# Single query, YARN, return headers
spark-sql \
  --master yarn \
  --executor-memory 8g \
  --num-executors 10 \
  --conf "spark.hadoop.hive.cli.print.header=true" \
  -e "SELECT region, COUNT(*) FROM sales GROUP BY region"

# Script with Iceberg support via --packages
spark-sql \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=hive \
  -f /path/to/iceberg_queries.sql

# Start on a specific database, with initialisation SQL
spark-sql \
  --database analytics \
  -i /path/to/session_init.sql

# Kerberos-authenticated YARN session
spark-sql \
  --master yarn \
  --principal spark@REALM.COM \
  --keytab /etc/security/spark.keytab
```

---

## :material-cog: Useful `--conf` Properties

| Property | Default | Description |
|----------|---------|-------------|
| `spark.sql.adaptive.enabled` | `true` (3.2+) | Enable Adaptive Query Execution |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Auto-coalesce shuffle partitions |
| `spark.sql.shuffle.partitions` | `200` | Number of post-shuffle partitions |
| `spark.hadoop.hive.cli.print.header` | `false` | Print column headers in output |
| `spark.sql.ansi.enabled` | `false` | Enable strict ANSI SQL mode |
| `spark.sql.warehouse.dir` | `./spark-warehouse` | Location for managed tables |
| `spark.sql.catalogImplementation` | `in-memory` | `hive` to use Hive Metastore |
| `spark.hadoop.hive.metastore.uris` | — | Thrift URI for Hive Metastore |
| `spark.sql.extensions` | — | Comma-separated catalog/session extensions |
| `spark.sql.defaultCatalog` | `spark_catalog` | Default catalog on startup |
| `spark.driver.extraJavaOptions` | — | Extra JVM flags for the driver |

---

## :material-magnify: Behavior Notes

1. **`-e` and `-f` are mutually exclusive** — use one or the other per invocation.
2. **`-i` runs before the prompt** — useful to `SET` variables or create temp views shared across an interactive session.
3. **`--conf` takes precedence** — flags on the command line override `spark-defaults.conf` and `hive-site.xml`.
4. **`--packages` downloads at startup** — Maven resolution happens before the SparkSession starts; use a local Maven repo for air-gapped environments.
