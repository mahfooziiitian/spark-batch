# PySpark Shell — Interactive Mode

`pyspark` is a REPL (Read-Eval-Print Loop) that gives you a live `SparkSession`
and `SparkContext` without writing a script file first. It is ideal for quick
data exploration.

## Start the shell

### Local (no cluster)

```bash
pyspark --master local[*]
```

### Against a YARN cluster

```bash
pyspark --master yarn --deploy-mode client
```

### With extra Python packages

```bash
pyspark --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
```

Once started you will see the prompt:

```
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.x.x
      /_/

Using Python version 3.x.x
SparkSession available as 'spark'.
SparkContext available as 'sc'.
>>>
```

## Example session

Paste or type these commands at the `>>>` prompt:

```python
# --- Step 1: create data ---
data = [("Alice", "Engineering", 95000),
        ("Bob",   "Marketing",   72000),
        ("Carol", "Engineering", 110000),
        ("Dave",  "HR",          65000),
        ("Eve",   "Marketing",   80000)]

df = spark.createDataFrame(data, ["name", "dept", "salary"])
df.show()

# --- Step 2: filter ---
df.filter(df.salary > 80000).show()

# --- Step 3: group and aggregate ---
from pyspark.sql import functions as F
df.groupBy("dept").agg(F.avg("salary").alias("avg_salary")).show()

# --- Step 4: SQL ---
df.createOrReplaceTempView("employees")
spark.sql("SELECT dept, MAX(salary) AS max_salary FROM employees GROUP BY dept").show()

# --- Step 5: write to parquet ---
df.write.mode("overwrite").parquet("/tmp/employees")

# --- Step 6: check Spark UI ---
print("Spark UI:", sc.uiWebUrl)
```

## Useful shell shortcuts

| Command | Description |
|---------|-------------|
| `spark` | Active `SparkSession` |
| `sc` | Active `SparkContext` |
| `sqlContext` | Legacy `SQLContext` (wraps `spark`) |
| `Ctrl-D` | Exit the shell |
| `spark.catalog.listTables()` | List registered temp views |

## Running a script from inside the shell

```python
exec(open("spark-submit/word_count.py").read())
```

## Tip: Increase executor memory

```bash
pyspark --master local[*] \
        --driver-memory 4g \
        --executor-memory 4g
```
