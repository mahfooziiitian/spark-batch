# Spark Options

List every configuration key-value pair active in the current SparkSession.
This is useful for debugging and verifying that your settings took effect.

## How It Works

Spark stores all configuration in the `SparkConf` object attached to the
`SparkContext`. Calling `getConf().getAll()` returns every key as a tuple.

```python
import os

from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("config-option")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))  # (1)!
         .config("spark.sql.adaptive.enabled", "true")        # (2)!
         .config("spark.ui.enabled", "false")
         .getOrCreate())

for key, value in sorted(spark.sparkContext.getConf().getAll()):
    print(f"{key} = {value}")
```

1. Environment-agnostic — runs locally or on a cluster without code changes.
2. AQE is enabled by default in Spark 3.2+ but explicit is clearer.

## Run

```bash
uv run python src/cfg/option/config_parser/config_option.py
```

## Common Config Keys

| Key | Description | Example |
|-----|-------------|---------|
| `spark.app.name` | Application name shown in the Spark UI | `"my-job"` |
| `spark.master` | Cluster manager URL | `"local[*]"`, `"yarn"` |
| `spark.executor.memory` | Memory per executor | `"2g"` |
| `spark.sql.shuffle.partitions` | Number of partitions after a shuffle | `"200"` |
| `spark.sql.adaptive.enabled` | Enable Adaptive Query Execution | `"true"` |
| `spark.ui.enabled` | Whether to start the Spark Web UI | `"true"` |

## Full Example

```python title="src/cfg/option/config_parser/config_option.py"
--8<-- "src/cfg/option/config_parser/config_option.py"
```

!!! tip
    Pipe the output through `grep` to find a specific key:

    ```bash
    uv run python src/cfg/option/config_parser/config_option.py 2>/dev/null | grep shuffle
    ```
