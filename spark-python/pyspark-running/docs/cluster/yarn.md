# YARN Cluster

YARN (Yet Another Resource Negotiator) is the resource manager bundled with Hadoop.
Most on-premise Spark deployments run on YARN.

## Deploy modes

| Mode | Driver location | Best for |
|------|-----------------|---------|
| `client` | Your edge node (local) | Interactive debugging |
| `cluster` | A YARN container (remote) | Production batch jobs |

!!! warning
    In **cluster** mode the driver runs inside a YARN container. Logs are not
    streamed to your terminal — use `yarn logs` to retrieve them after the job.

## Prerequisites

- Hadoop + YARN installed and reachable
- `HADOOP_CONF_DIR` or `YARN_CONF_DIR` pointing to cluster config files
- PySpark installed on the edge node

```bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
export SPARK_HOME=/opt/spark
```

## Submit examples

=== "Client mode"
    ```bash
    spark-submit \
      --master yarn \
      --deploy-mode client \
      --num-executors 4 \
      --executor-cores 2 \
      --executor-memory 4g \
      --driver-memory 2g \
      cluster/yarn/yarn_example.py
    ```

=== "Cluster mode"
    ```bash
    spark-submit \
      --master yarn \
      --deploy-mode cluster \
      --num-executors 8 \
      --executor-cores 4 \
      --executor-memory 8g \
      --driver-memory 4g \
      --conf spark.yarn.maxAppAttempts=2 \
      cluster/yarn/yarn_example.py
    ```

=== "With virtualenv"
    ```bash
    venv-pack -o pyspark_env.tar.gz

    spark-submit \
      --master yarn \
      --deploy-mode cluster \
      --archives pyspark_env.tar.gz#environment \
      --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
      cluster/yarn/yarn_example.py
    ```

=== "With py-files"
    ```bash
    spark-submit \
      --master yarn \
      --deploy-mode cluster \
      --py-files utils.zip \
      cluster/yarn/yarn_example.py
    ```

## Monitor jobs

```bash
# List running applications
yarn application -list

# Stream logs (cluster mode)
yarn logs -applicationId application_<timestamp>_<id>

# Kill a job
yarn application -kill application_<timestamp>_<id>
```

## Common YARN configuration

| Config key | Description |
|------------|-------------|
| `spark.yarn.queue` | Submit to a specific YARN queue |
| `spark.yarn.maxAppAttempts` | Retry count on driver failure |
| `spark.dynamicAllocation.enabled` | Scale executors automatically |
| `spark.shuffle.service.enabled` | Required for dynamic allocation |
| `spark.yarn.executor.memoryOverhead` | Off-heap memory per executor (MB) |

## SparkSession for YARN

```python
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("yarn-job")
         # master() is overridden by --master yarn at submit time
         .config("spark.yarn.queue", os.environ.get("YARN_QUEUE", "default"))
         .config("spark.sql.adaptive.enabled", "true")           # (1)!
         .config("spark.dynamicAllocation.enabled", "true")      # (2)!
         .config("spark.shuffle.service.enabled", "true")
         .getOrCreate())
```

1. AQE automatically coalesces small shuffle partitions — highly recommended on YARN.
2. Lets YARN scale executors up/down based on workload.

## Example — `yarn_example.py`

```python title="cluster/yarn/yarn_example.py"
--8<-- "cluster/yarn/yarn_example.py"
```
