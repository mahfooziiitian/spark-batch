# findspark

Use [`findspark`](https://pypi.org/project/findspark/) to locate and initialise the
Spark installation so that `import pyspark` works in any Python environment — scripts,
notebooks, or IDEs — without manually setting `SPARK_HOME`.

## How It Works

`findspark.init()` searches for Spark (via `SPARK_HOME`, `PATH`, or common install
locations) and adds the PySpark libraries to `sys.path`.

```python
import findspark

findspark.init()           # (1)!

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
```

1. After this call, `import pyspark` works even if `SPARK_HOME` is not in your shell profile.

## Prerequisites

=== "uv"
    ```bash
    uv add findspark
    ```

=== "pip"
    ```bash
    pip install findspark~=2.0.1
    ```

## Environment Variables

The example script optionally reads Hive / Derby paths for warehouse setup:

| Variable | Purpose | Required |
|----------|---------|----------|
| `SPARK_HOME` | Path to the Spark installation | Detected automatically |
| `DERBY_HOME` | Path to the Apache Derby installation | Optional |
| `SPARK_WAREHOUSE` | Path to the Spark SQL warehouse directory | Optional |

## Run

```bash
uv run python src/cfg/library/find_spark_lib.py
```

Expected output:

```
SPARK_HOME          = /opt/spark
Spark version       = 3.5.0
findspark OK
```

## Full Example

```python title="src/cfg/library/find_spark_lib.py"
--8<-- "src/cfg/library/find_spark_lib.py"
```

!!! tip
    If you install PySpark via `pip` or `uv`, `findspark` is usually unnecessary — the
    package is already on the Python path. It is most useful when Spark is installed
    system-wide (e.g. via tarball) and not managed by `pip`.
