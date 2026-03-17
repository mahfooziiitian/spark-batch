# PySpark — Local Mode

Local mode runs Spark entirely on your laptop/workstation. No cluster is needed.
It is the fastest way to develop and test PySpark jobs.

## How it works

Spark spawns all executor threads inside the same JVM process as the driver.
`local[*]` uses one thread per CPU core. `local[2]` forces exactly 2 threads.

```
┌────────────────────────────────┐
│  Your machine                  │
│  ┌──────────────────────────┐  │
│  │  Driver  +  Executors    │  │
│  │  (single JVM process)    │  │
│  └──────────────────────────┘  │
└────────────────────────────────┘
```

## Prerequisites

```bash
pip install pyspark          # installs Spark + PySpark bindings
```

Java 8, 11, or 17 must be on your `PATH`:

```bash
java -version
```

## Run the example

```bash
python local_example.py
```

## Key SparkSession options for local mode

| Option | Purpose |
|--------|---------|
| `.master("local[*]")` | Use all available CPU cores |
| `.master("local[1]")` | Single-threaded (deterministic for tests) |
| `spark.sql.shuffle.partitions` | Lower from default 200 for small data |
| `spark.ui.enabled` | Set `false` to skip the Spark UI |

## When to use

- Writing and debugging new jobs
- Unit / integration tests in CI pipelines
- Learning PySpark without a cluster

