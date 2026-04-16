# Java Setup

PySpark 3.5.x with Arrow requires **Java 11** (or 17). Java 21 breaks the Arrow
memory allocator (`sun.misc.Unsafe` restriction).

## Check Your Java Version

```bash
java -version
```

!!! failure "Java 21 — not compatible"

    ```
    openjdk version "21.0.x" ...
    ```

    Arrow operations like `toPandas()`, `mapInPandas()`, and Pandas UDFs will crash
    with `sun.misc.Unsafe or java.nio.DirectByteBuffer.<init>(long, int) not available`.

!!! success "Java 11 — recommended"

    ```
    openjdk version "11.0.x" ...
    ```

## Install Java 11

=== "SDKMAN (recommended)"

    ```bash
    curl -s "https://get.sdkman.io" | bash
    source "$HOME/.sdkman/bin/sdkman-init.sh"
    sdk install java 11.0.30-amzn
    ```

=== "Ubuntu / Debian"

    ```bash
    sudo apt-get install -y openjdk-11-jdk-headless
    ```

=== "macOS"

    ```bash
    brew install openjdk@11
    ```

## Automatic Java Detection

This project includes `spark_env.py` which auto-detects Java 11/17 via SDKMAN
before the JVM starts:

```python title="src/psa/spark_env.py"
--8<-- "src/psa/spark_env.py"
```

Import it before PySpark in scripts and notebooks:

```python
import spark_env  # must be first — sets JAVA_HOME before JVM starts

from pyspark.sql import SparkSession
```

## Manual Override

Set `JAVA_HOME` explicitly:

```bash
export JAVA_HOME="$HOME/.sdkman/candidates/java/11.0.30-amzn"
python src/psa/pyspark_pyarrow.py
```

## Configuration Reference

| Env Variable | Purpose | Example |
|-------------|---------|---------|
| `JAVA_HOME` | JDK location | `~/.sdkman/candidates/java/11.0.30-amzn` |
| `PYSPARK_PYTHON` | Worker Python binary | `python3` |
| `PYSPARK_DRIVER_PYTHON` | Driver Python binary | `python3` |
| `SPARK_LOCAL_IP` | Bind address for local mode | `127.0.0.1` |
