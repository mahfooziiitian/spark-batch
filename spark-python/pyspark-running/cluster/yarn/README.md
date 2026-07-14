# PySpark on YARN Cluster

YARN (Yet Another Resource Negotiator) is the resource manager bundled with
Hadoop. Most on-premise Spark deployments run on YARN.

## Deploy modes

| Mode | Driver location | Best for |
|------|-----------------|---------|
| `client` | Your edge node (local) | Interactive / debugging |
| `cluster` | A YARN container (remote) | Production batch jobs |

## Prerequisites

- Hadoop + YARN installed and reachable
- `HADOOP_CONF_DIR` or `YARN_CONF_DIR` pointing to cluster config files
- PySpark installed on the edge node

```bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
export SPARK_HOME=/opt/spark
```

## Submit examples

### Client mode (driver on edge node)

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors 4 \
  --executor-cores 2 \
  --executor-memory 4g \
  --driver-memory 2g \
  yarn_example.py
```

### Cluster mode (driver on YARN)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 8 \
  --executor-cores 4 \
  --executor-memory 8g \
  --driver-memory 4g \
  --conf spark.yarn.maxAppAttempts=2 \
  yarn_example.py
```

### Shipping Python dependencies to executors

```bash
# Option 1: zip a virtualenv
venv-pack -o pyspark_env.tar.gz
spark-submit \
  --master yarn \
  --archives pyspark_env.tar.gz#environment \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
  yarn_example.py

# Option 2: extra py-files
spark-submit --master yarn --py-files utils.zip yarn_example.py
```

## Monitor jobs

```bash
# List running applications
yarn application -list

# Get logs for a finished app
yarn logs -applicationId application_<id>
```

## Common YARN configs

| Config | Description |
|--------|-------------|
| `spark.yarn.queue` | Submit to a specific YARN queue |
| `spark.yarn.maxAppAttempts` | Retry attempts on failure |
| `spark.dynamicAllocation.enabled` | Let YARN scale executors dynamically |
| `spark.shuffle.service.enabled` | Required for dynamic allocation |

## Run the example

```bash
# Local test first
spark-submit --master local[*] yarn_example.py

# Then submit to YARN
spark-submit --master yarn --deploy-mode cluster yarn_example.py
```
