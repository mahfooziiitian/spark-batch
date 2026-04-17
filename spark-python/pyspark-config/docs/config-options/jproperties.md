# jproperties

Read and write Java-style `.properties` files using the
[`jproperties`](https://pypi.org/project/jproperties/) library. This is useful
when sharing configuration between PySpark (Python) and Spark (Scala/Java) codebases.

## How It Works

The `jproperties` library loads `.properties` files into a dict-like object. Each
value is a `PropertyTuple` with `.data` and `.meta` attributes.

```python
from jproperties import Properties

p = Properties()
with open("config.properties", "rb") as f:
    p.load(f, "utf-8")

value = p["key1"].data  # "value1"
```

## Prerequisites

=== "uv"
    ```bash
    uv add jproperties
    ```

=== "pip"
    ```bash
    pip install jproperties~=2.1.1
    ```

## Reusable PropertiesHandler

The project includes a reusable `PropertiesHandler` class that wraps read/write
operations:

```python title="src/cfg/option/config_jproperties/config_jproperties.py"
--8<-- "src/cfg/option/config_jproperties/config_jproperties.py"
```

### Usage

```python
from cfg.option.config_jproperties.config_jproperties import PropertiesHandler

handler = PropertiesHandler("cfg/config.properties")
props = handler.read_properties()
print(props["key1"].data)

props["new_key"] = "new_value"
handler.write_properties(props)
```

## Run

```bash
uv run python src/cfg/option/config_parser/config_file_jproperties.py
```

Expected output:

```
Loaded 5 properties from .../cfg/config.properties
--------------------------------------------------
  spark.app.name = pyspark-config
  spark.master = local[*]
  spark.executor.memory = 2g
  spark.driver.memory = 1g
  spark.sql.shuffle.partitions = 4

Applied spark.sql.shuffle.partitions = 4
```

## Full Example with Spark

```python title="src/cfg/option/config_parser/config_file_jproperties.py"
--8<-- "src/cfg/option/config_parser/config_file_jproperties.py"
```

!!! note
    The script loads properties and applies them to the running Spark session
    using `spark.conf.set()` — a pattern used in production to externalise config.

!!! success "Good fit"
    - Java `.properties` compatibility
    - Read and write support
    - Works across Python and JVM Spark codebases

!!! failure "Not a good fit"
    - No variable interpolation
    - No sections or hierarchy — flat key-value only
