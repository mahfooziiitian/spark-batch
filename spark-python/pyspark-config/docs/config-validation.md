# SparkConf Validation

Create a `SparkConf` object programmatically, pass it to `SparkSession.builder`,
and then retrieve it back to verify the settings.

## How It Works

```mermaid
graph LR
    A[SparkConf] -->|passed to| B[SparkSession.builder]
    B -->|creates| C[SparkSession]
    C -->|sparkContext.getConf| D[Retrieved SparkConf]
```

Instead of setting config keys one at a time on the builder, you can construct a
`SparkConf` object up front and feed it in a single `.config(conf=conf)` call.
This is handy when configs are loaded from an external source.

```python
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = (SparkConf()
        .setAppName("config-validation")
        .setMaster(os.environ.get("SPARK_MASTER", "local[*]"))   # (1)!
        .set("spark.executor.memory", "2g")
        .set("spark.sql.adaptive.enabled", "true"))              # (2)!

spark = SparkSession.builder.config(conf=conf).getOrCreate()
```

1. Environment-agnostic — runs locally or on a cluster without code changes.
2. Enables Adaptive Query Execution for better shuffle performance.

## Run

```bash
uv run python src/cfg/validation/config_validation.py
```

Expected output:

```
spark.app.name       = config-validation
spark.master         = local[*]
spark.executor.memory= 2g
shuffle.partitions   = 4
adaptive.enabled     = true
spark.speculation    = false
```

## Configuration Reference

| Method | Purpose |
|--------|---------|
| `SparkConf().setAppName(name)` | Set the application name |
| `SparkConf().setMaster(url)` | Set the master URL |
| `SparkConf().set(key, value)` | Set any Spark config property |
| `spark.sparkContext.getConf()` | Retrieve the active config |
| `conf.get(key)` | Read a single config value |
| `conf.get(key, default)` | Read with a fallback default |
| `conf.getAll()` | List all config key-value pairs |

## Full Example

```python title="src/cfg/validation/config_validation.py"
--8<-- "src/cfg/validation/config_validation.py"
```

## Print All Spark Config

A companion script dumps every config key using both `getConf().getAll()` and
`spark.sql("SET -v")`:

```bash
uv run python src/cfg/validation/print_all_spark_config.py
```

??? example "Full source"

    ```python title="src/cfg/validation/print_all_spark_config.py"
    --8<-- "src/cfg/validation/print_all_spark_config.py"
    ```

!!! success "Good fit"
    - Building config from external sources (databases, vaults, APIs)
    - Validating settings before job submission
    - Sharing a single `SparkConf` across multiple sessions

!!! failure "Not a good fit"
    - Simple jobs with only a few config keys — use `.config()` on the builder directly
