# Environment Variables Reference

## Always Required

| Variable | Purpose | Example |
|----------|---------|---------|
| `JAVA_HOME` | Path to the JDK | `/usr/lib/jvm/java-11-openjdk-amd64` |
| `PYSPARK_PYTHON` | Python binary for **executors** | `python3` or `/usr/bin/python3` |
| `PYSPARK_DRIVER_PYTHON` | Python binary for the **driver** | `python3` |

!!! warning
    `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` must point to the **same**
    Python version. A mismatch causes a worker serialisation error.

## Spark Binary (tarball installs)

| Variable | Purpose | Example |
|----------|---------|---------|
| `SPARK_HOME` | Path to the Spark installation | `/opt/spark` |
| `PATH` | Must include `$SPARK_HOME/bin` | `$PATH:$SPARK_HOME/bin` |

## Local Development

| Variable | Purpose | Default |
|----------|---------|---------|
| `SPARK_LOCAL_IP` | Bind IP for the driver | `127.0.0.1` (recommended) |
| `SPARK_WAREHOUSE` | Spark SQL warehouse directory | `/tmp/spark-warehouse` |
| `DERBY_HOME` | Embedded Derby metastore home | `/tmp/derby` |

## YARN / Hadoop

| Variable | Purpose | Example |
|----------|---------|---------|
| `HADOOP_CONF_DIR` | Hadoop config directory | `/etc/hadoop/conf` |
| `YARN_CONF_DIR` | YARN config directory | `/etc/hadoop/conf` |
| `SPARK_MASTER` | Override master URL | `yarn` |
| `YARN_QUEUE` | Target YARN queue | `default` |

## Job Input / Output (per-script)

| Variable | Purpose | Default |
|----------|---------|---------|
| `INPUT_PATH` | Path / URI to input data | `None` → in-memory sample |
| `OUTPUT_PATH` | Path / URI for output | `/tmp/<job>_output` |

## Proxy (corporate networks)

```bash
export HTTP_PROXY=http://proxy-host:port
export HTTPS_PROXY=http://proxy-host:port
# or pass directly to pip:
pip install pyspark --proxy="http://proxy-host:port"
```

## Quick Export Snippet

```bash
# Paste into ~/.bashrc or ~/.zshrc
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
export PYSPARK_PYTHON=$(which python3)
export PYSPARK_DRIVER_PYTHON=$(which python3)
export SPARK_LOCAL_IP=127.0.0.1
```
