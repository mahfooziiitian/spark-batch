# :material-cog: Spark SQL Configuration

Spark SQL configuration settings control execution behaviour, performance, memory,
and feature flags. Settings can be applied at session scope (SQL `SET`) or cluster
scope (cluster config / `SparkConf`).

### :material-animation-play: Interactive Visualization — Configuration Precedence

Switch between the cluster default, a session override, and a reset to see which value
Spark treats as effective. The example uses `spark.sql.shuffle.partitions`, but the same
precedence rules apply to most runtime SQL settings.

<div id="viz-config-overview" class="ts-viz"></div>

<script src="../assets/js/configuration-viz.js"></script>

______________________________________________________________________

## :material-sitemap: In This Section

| Page                                 | Covers                                                            |
| ------------------------------------ | ----------------------------------------------------------------- |
| [Shuffle & Partitioning](shuffle.md) | `shuffle.partitions`, `partitionOverwriteMode`, repartition hints |
| [Adaptive Query Execution](aqe.md)   | AQE settings — skew join, coalesce, dynamic partition pruning     |
| [Join Configuration](join.md)        | Broadcast threshold, join strategy hints, sort-merge config       |
| [Memory & Spill](memory.md)          | Executor memory, spill settings, off-heap                         |
| [File & I/O Settings](io.md)         | Parquet/ORC/Delta read-write settings, compression, file size     |
| [Session Management](session.md)     | `SET`, `RESET`, `SET -v`, scoping rules                           |

______________________________________________________________________

## :material-code-tags: Quick Reference

```sql
-- View all current settings
SET -v;

-- View a single setting
SET spark.sql.shuffle.partitions;

-- Set for the current session
SET spark.sql.shuffle.partitions = 50;

-- Reset a setting to its default
RESET spark.sql.shuffle.partitions;
```

______________________________________________________________________

## :material-layers-outline: Configuration Precedence

| Level            | How it is applied                               | Typical examples                              | Can SQL `SET` change it now?                |
| ---------------- | ----------------------------------------------- | --------------------------------------------- | ------------------------------------------- |
| Built-in default | Compiled into Spark                             | `spark.sql.adaptive.enabled = true`           | Yes, if the setting is dynamic              |
| Cluster / app    | `spark-defaults.conf`, submit args, `SparkConf` | `spark.sql.shuffle.partitions = 400`          | Session `SET` can only override dynamically |
| Session override | `SET key = value`                               | `SET spark.sql.shuffle.partitions = 50`       | Yes — current session only                  |
| Query hint       | SQL comment hint                                | `/*+ BROADCAST(dim) */`, `/*+ COALESCE(8) */` | Affects one query only                      |

Spark always resolves the **effective** value from the highest-precedence scope that is
currently in play. `SET key` shows that effective value rather than every layer that led
to it, which is why `RESET key` is the safest way to return to the cluster or built-in
baseline after an experiment.

______________________________________________________________________

## :material-information-outline: Scoping Rules

1. **Cluster-level** config (set in `spark-defaults.conf` or cluster UI) applies to all sessions.
2. **Session-level** `SET` overrides cluster config for the current SparkSession only — reset when the session ends.
3. `RESET key` reverts a session-level override back to the cluster/default value.
4. `RESET` (no argument) resets **all** session-level overrides.
5. Some settings (for example, `spark.executor.memory`) are **static** — they cannot be changed after the SparkContext starts.

______________________________________________________________________

## :material-tune: Static vs Dynamic Settings

Dynamic SQL settings are the ones you tune most often when diagnosing query behaviour:
`spark.sql.shuffle.partitions`, `spark.sql.autoBroadcastJoinThreshold`,
`spark.sql.adaptive.enabled`, and many file-scan settings can all be overridden with
session-scoped `SET` commands.

Static infrastructure settings such as `spark.executor.memory`, `spark.executor.cores`,
or the number of executors must be chosen before the application starts. SQL can still
**read** them with `SET key`, but changing them requires a new Spark application,
notebook cluster restart, or different submit-time configuration.

______________________________________________________________________

## :material-playlist-check: Safe Tuning Workflow

```sql
-- 1) Inspect the current effective value
SET spark.sql.autoBroadcastJoinThreshold;

-- 2) Override only for this session
SET spark.sql.autoBroadcastJoinThreshold = 209715200;  -- 200 MB

-- 3) Run the query and inspect the plan
EXPLAIN
SELECT f.sale_id, d.category
FROM fact_sales AS f
JOIN dim_product AS d ON f.product_id = d.product_id;

-- 4) Reset when finished
RESET spark.sql.autoBroadcastJoinThreshold;
```

That inspect → override → verify → reset loop keeps long-lived notebook or SQL-editor
sessions predictable, which is especially important when multiple experiments happen in
the same SparkSession.
