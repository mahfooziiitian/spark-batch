# :material-format-list-bulleted: Options Reference

`spark-sql` has two layers of command-line inputs: **CLI-specific flags** such as `-e`, `-f`, `-i`, and `--hivevar`, plus the familiar Spark launcher options used to size and place the underlying Spark application.

That split matters in practice: one group chooses how SQL enters the shell, while the other group controls where the driver runs, how much memory it gets, and which extra dependencies are available when the session starts.

### :material-animation-play: Interactive Visualization — Invocation Mode Explorer

Switch between interactive, inline, script-file, and initialised sessions to compare which input source Spark reads, when the prompt appears, and when the process exits.

<div id="viz-cli-options" class="ts-viz"></div>

<script src="../../assets/js/configuration-cli-viz.js"></script>

______________________________________________________________________

## :material-console: Syntax

```bash
spark-sql [cli-options] [spark-launcher-options] [--conf key=value ...]
```

______________________________________________________________________

## :material-table: CLI-Specific Flags

| Flag              | Argument        | Description                                                   |
| ----------------- | --------------- | ------------------------------------------------------------- |
| `-e`              | `<sql>`         | Execute SQL passed on the command line and exit               |
| `-f`              | `<file>`        | Execute SQL from a file and exit                              |
| `-i`              | `<file>`        | Run an initialization file before the prompt or before `-f`   |
| `--database`      | `<db>`          | Set the active database on startup                            |
| `-S`, `--silent`  | —               | Reduce banner and progress output in shell or batch usage     |
| `-v`, `--verbose` | —               | Echo SQL statements as they execute                           |
| `--hivevar`       | `<key>=<value>` | Define a substitution variable referenced as `${hivevar:key}` |
| `-d`, `--define`  | `<key>=<value>` | Hive-style variable substitution referenced as `${key}`       |
| `--hiveconf`      | `<key>=<value>` | Set a Hive property, also addressable as `${hiveconf:key}`    |
| `-H`, `--help`    | —               | Print help and exit                                           |

______________________________________________________________________

## :material-server: Common Spark Launcher Options

`spark-sql` also inherits the standard Spark launcher flags used by `spark-submit`-style entry points.

| Flag                | Argument          | Description                                              |
| ------------------- | ----------------- | -------------------------------------------------------- |
| `--master`          | `<url>`           | Spark master URL such as `local[*]`, `yarn`, or `k8s://` |
| `--deploy-mode`     | `client\|cluster` | Driver placement for supported cluster managers          |
| `--driver-memory`   | `<mem>`           | Driver JVM memory, for example `4g`                      |
| `--driver-cores`    | `<n>`             | Driver cores in cluster mode                             |
| `--executor-memory` | `<mem>`           | Executor JVM memory                                      |
| `--executor-cores`  | `<n>`             | Cores per executor                                       |
| `--num-executors`   | `<n>`             | Number of executors on YARN or Kubernetes                |
| `--jars`            | `<paths>`         | Extra JARs added to the classpath                        |
| `--packages`        | `<coords>`        | Maven coordinates resolved before the session starts     |
| `--repositories`    | `<urls>`          | Extra repositories for `--packages`                      |
| `--files`           | `<paths>`         | Files shipped to executors                               |
| `--name`            | `<name>`          | Application name shown in Spark UI                       |
| `--queue`           | `<queue>`         | YARN queue name                                          |
| `--principal`       | `<principal>`     | Kerberos principal                                       |
| `--keytab`          | `<file>`          | Kerberos keytab                                          |
| `--version`         | —                 | Print Spark version and exit                             |

______________________________________________________________________

## :material-compare: Mode-Oriented Examples

```bash
# Interactive shell
spark-sql --master local[*]

# Single statement and exit
spark-sql -S -e "SHOW TABLES IN default"

# Run a script from local disk
spark-sql -f ./sql/daily_report.sql

# Run a script from HDFS or object storage
spark-sql -f hdfs:///analytics/sql/daily_report.sql

# Preload settings and temp views before the prompt appears
spark-sql --database analytics -i ./sql/session_init.sql

# Run a batch file with JDBC driver coordinates available at startup
spark-sql   --packages org.postgresql:postgresql:42.7.4   -f ./sql/jdbc_checks.sql
```

______________________________________________________________________

## :material-variable: Variable and Property Injection

```bash
spark-sql   --hivevar START_DATE=2026-01-01   --hivevar END_DATE=2026-01-31   --hiveconf hive.exec.dynamic.partition.mode=nonstrict   -e "SELECT '${hivevar:START_DATE}', '${hivevar:END_DATE}'"
```

Use `--hivevar` when you want parameter substitution inside SQL text and `--hiveconf` when you need Hive behavior or metastore-related settings at startup.

______________________________________________________________________

## :material-file-document-outline: Path Interpretation

For `-i` and `-f`, paths without a URI scheme are treated as local files. If you need Spark to read initialization or SQL files from distributed storage, use an explicit scheme such as `hdfs://` or `s3://`.

| Input                     | Interpretation    |
| ------------------------- | ----------------- |
| `./sql/init.sql`          | Local file        |
| `/opt/sql/report.sql`     | Local file        |
| `file:///opt/sql/run.sql` | Local file        |
| `hdfs:///shared/run.sql`  | HDFS path         |
| `s3://bucket/run.sql`     | Object-store path |

______________________________________________________________________

## :material-magnify: Behavior Notes

1. **`-e` and `-f` are mutually exclusive** — use one primary SQL input mode per invocation.
2. **`-i` runs first** — it executes before the interactive prompt appears and before a `-f` script is processed.
3. **Launcher options apply before SQL parsing begins** — memory, executors, packages, and master selection must be in place before Spark creates the session.
4. **`--silent` is handy for captured output** — it reduces non-query chatter when redirecting results into files or downstream tools.
