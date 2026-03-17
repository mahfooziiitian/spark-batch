---
applyTo: "src/**/*.py"
---

# PySpark Architecture — Source Code Instructions

## SparkSession (`src/spark_session.py`)

Always use the standard env-var-driven pattern so scripts run locally without
modification and on a cluster without code changes:

```python
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("pyspark-architecture-demo")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

### SparkSession ↔ SparkContext relationship

```python
# Access the underlying context — never instantiate SparkContext directly
sc = spark.sparkContext

# Multiple sessions share a single SparkContext
session_a = SparkSession.builder.appName("A").getOrCreate()
session_b = session_a.newSession()      # new catalog & SQL config, shared SC
assert session_a.sparkContext is session_b.sparkContext
```

### `getOrCreate()` singleton guarantee

```python
# Safe to call many times — always returns the same session
spark1 = SparkSession.builder.appName("first").master("local[*]").getOrCreate()
spark2 = SparkSession.builder.appName("second").master("local[*]").getOrCreate()
assert spark1 is spark2   # same object
```

---

## SparkContext (`src/spark_driver.py` or standalone scripts)

Use `SparkContext` only when demonstrating RDD-level APIs or low-level
architecture behaviour:

```python
from pyspark import SparkConf, SparkContext

conf = (SparkConf()
        .setAppName("rdd-demo")
        .setMaster(os.environ.get("SPARK_MASTER", "local[*]")))
sc = SparkContext.getOrCreate(conf)

rdd = sc.parallelize(range(1, 6), numSlices=2)
print(f"Partitions: {rdd.getNumPartitions()}")   # shows physical partition count
print(f"Sum: {rdd.sum()}")
sc.stop()
```

### Always use `getOrCreate()` — never the bare constructor

```python
# BAD — raises ValueError if a SparkContext already exists in the JVM
sc = SparkContext(conf=conf)

# GOOD
sc = SparkContext.getOrCreate(conf)
```

---

## Driver Patterns (`src/spark_driver.py`)

The Driver is where the Spark application `main()` runs. Document driver
responsibilities clearly:

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main() -> None:
    spark = (SparkSession.builder
             .appName("driver-demo")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # Driver builds the logical plan; executors run the tasks
    df = spark.range(0, 1_000_000, numPartitions=4)
    result = (df
              .withColumn("squared", F.col("id") * F.col("id"))
              .filter(F.col("squared") % 3 == 0)
              .agg(F.sum("squared").alias("total")))

    result.show()
    spark.stop()

if __name__ == "__main__":
    main()
```

---

## Executor Patterns (`src/spark_executor.py`)

Use executor examples to illustrate partitioning, task locality, caching,
and shuffle behaviour:

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def demo_executor_partitioning(spark: SparkSession) -> None:
    """Show how data is distributed across executor partitions."""
    df = spark.range(0, 100, numPartitions=4)

    # mapPartitionsWithIndex runs one lambda per partition (on the executor)
    def show_partition(idx: int, rows):
        for row in rows:
            yield (idx, row["id"])

    partitioned = df.rdd.mapPartitionsWithIndex(show_partition)
    for partition_id, value in partitioned.take(8):
        print(f"Partition {partition_id}: {value}")

def demo_caching(spark: SparkSession) -> None:
    """Demonstrate executor-side caching."""
    df = spark.range(0, 10_000).cache()
    df.count()                     # materialise the cache on executors
    print(df.count())              # served from executor memory
    df.unpersist()

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("executor-demo")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    demo_executor_partitioning(spark)
    demo_caching(spark)
    spark.stop()
```

---

## Cluster Manager Configs

When demonstrating cluster managers, drive all master URLs from env vars:

```python
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")

# YARN
# export SPARK_MASTER=yarn

# Kubernetes
# export SPARK_MASTER=k8s://https://<k8s-api-server>:6443

# Standalone
# export SPARK_MASTER=spark://<master-host>:7077
```

Include the relevant extra configs per environment:

```python
builder = SparkSession.builder.appName("cluster-demo").master(SPARK_MASTER)

if SPARK_MASTER == "yarn":
    builder = (builder
               .config("spark.yarn.queue", os.environ.get("YARN_QUEUE", "default"))
               .config("spark.dynamicAllocation.enabled", "true")
               .config("spark.dynamicAllocation.minExecutors", "1")
               .config("spark.dynamicAllocation.maxExecutors", "10"))

elif SPARK_MASTER.startswith("k8s://"):
    builder = (builder
               .config("spark.kubernetes.container.image",
                       os.environ.get("SPARK_K8S_IMAGE", "apache/spark:3.5.0-python3"))
               .config("spark.kubernetes.namespace",
                       os.environ.get("SPARK_K8S_NAMESPACE", "default")))
```

---

## General Rules

- Use `from pyspark.sql import functions as F` — never `import *`.
- Chain transformations with `(df\n    .filter(...)\n    .groupBy(...)\n    .agg(...))`.
- Set `spark.sql.shuffle.partitions` to `"4"` for local examples.
- Call `spark.stop()` at the end of every standalone script.
- Log level: `"WARN"` in scripts, `"ERROR"` in tests.
