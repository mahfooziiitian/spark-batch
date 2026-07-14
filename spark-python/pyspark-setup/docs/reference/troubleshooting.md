# Troubleshooting

## `JAVA_HOME is not set`

```bash
# Linux / macOS
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))

# macOS (Homebrew)
export JAVA_HOME=$(brew --prefix openjdk@11)

# Windows (PowerShell)
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-11.x-hotspot"
```

!!! tip
    Add the export to `~/.bashrc` or `~/.zshrc` so it persists across sessions.

---

## `Python in worker has different version than driver`

Both variables must point to the **same** binary:

```bash
export PYSPARK_PYTHON=$(which python3)
export PYSPARK_DRIVER_PYTHON=$(which python3)
```

---

## `ModuleNotFoundError: No module named 'pyspark'` (Docker)

The `apache/spark` image does not install PySpark via pip. Running `python3 script.py`
directly bypasses the entrypoint's `PYTHONPATH` setup.

Fix — add to the Dockerfile:

```dockerfile
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"
```

---

## `ModuleNotFoundError` on Executors (cluster mode)

Executors don't have access to the driver's virtualenv.
Ship dependencies using one of these methods:

=== "venv-pack (YARN)"
    ```bash
    pip install venv-pack
    venv-pack -o pyspark_venv.tar.gz

    spark-submit --master yarn \
      --archives pyspark_venv.tar.gz#env \
      --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./env/bin/python \
      --conf spark.executorEnv.PYSPARK_PYTHON=./env/bin/python \
      my_job.py
    ```

=== "--py-files (small packages)"
    ```bash
    zip -r deps.zip my_package/
    spark-submit --master yarn --py-files deps.zip my_job.py
    ```

---

## `winutils.exe` not found (Windows)

Download `winutils.exe` matching your Hadoop version from
[steveloughran/winutils](https://github.com/steveloughran/winutils) and place it in
`%SPARK_HOME%\bin`:

```powershell
$env:HADOOP_HOME = "C:\apps\spark-3.5.0-bin-hadoop3"
$env:PATH       += ";$env:HADOOP_HOME\bin"
```

---

## `Address already in use` (port 4040)

The Spark UI tries to bind to port 4040. If another Spark session is running:

```python
# Disable the UI entirely (good for scripts and CI)
.config("spark.ui.enabled", "false")

# Or let Spark pick the next free port (4041, 4042, ...)
.config("spark.ui.port", "0")
```

---

## Slow First Run

PySpark downloads metadata on first run. To suppress:

```python
.config("spark.sql.shuffle.partitions", "4")   # avoids 200-partition shuffles on small data
.config("spark.ui.enabled", "false")            # skips Web UI initialisation
```

---

## Behind a Corporate Proxy

```bash
pip install pyspark --proxy="http://proxy-host:port"

# Or globally
export HTTP_PROXY=http://proxy-host:port
export HTTPS_PROXY=http://proxy-host:port
```
