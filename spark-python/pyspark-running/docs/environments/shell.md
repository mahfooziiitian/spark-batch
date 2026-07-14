# Interactive Shell

`pyspark` is a REPL (Read-Eval-Print Loop) that drops you into a live Python session
with `spark` and `sc` already initialised. No script file needed — ideal for ad-hoc
data exploration.

## Start the shell

=== "Local"
    ```bash
    pyspark --master local[*]
    ```

=== "YARN (client)"
    ```bash
    pyspark --master yarn --deploy-mode client
    ```

=== "With extra packages"
    ```bash
    pyspark --master local[*] \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
    ```

=== "With extra memory"
    ```bash
    pyspark --master local[*] \
        --driver-memory 4g \
        --executor-memory 4g
    ```

Once started you will see:

```
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.x.x
      /_/

SparkSession available as 'spark'.
SparkContext available as 'sc'.
>>>
```

## Example session

Paste these blocks at the `>>>` prompt one section at a time:

```python title="Step 1 — Create a DataFrame"
data = [("Alice", "Engineering", 95000),
        ("Bob",   "Marketing",   72000),
        ("Carol", "Engineering", 110000),
        ("Dave",  "HR",          65000),
        ("Eve",   "Marketing",   80000)]

df = spark.createDataFrame(data, ["name", "dept", "salary"])
df.show()
```

```python title="Step 2 — Filter"
df.filter(df.salary > 80000).show()
```

```python title="Step 3 — Aggregate"
from pyspark.sql import functions as F
df.groupBy("dept").agg(F.avg("salary").alias("avg_salary")).show()
```

```python title="Step 4 — SQL"
df.createOrReplaceTempView("employees")
spark.sql("SELECT dept, MAX(salary) AS max_salary FROM employees GROUP BY dept").show()
```

```python title="Step 5 — Write Parquet"
df.write.mode("overwrite").parquet("/tmp/employees")
```

```python title="Step 6 — Check the Spark UI"
print("Spark UI:", sc.uiWebUrl)
```

## Built-in variables

| Variable | Type | Description |
|----------|------|-------------|
| `spark` | `SparkSession` | Active session |
| `sc` | `SparkContext` | Low-level context |
| `sqlContext` | `SQLContext` | Legacy wrapper (avoid in new code) |

## Useful commands

| Command | Description |
|---------|-------------|
| `spark.catalog.listTables()` | List registered temp views |
| `df.printSchema()` | Show column names and types |
| `df.explain()` | Print the query execution plan |
| `spark.stop()` | Stop the session |
| `Ctrl-D` | Exit the shell |

## Run a script from inside the shell

```python
exec(open("spark-submit/word_count.py").read())
```

!!! tip
    Use the shell to prototype a transformation, then copy the working code
    into a `.py` script and submit it with `spark-submit`.
