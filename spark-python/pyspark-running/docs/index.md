# Running PySpark on Different Environments

PySpark can run in multiple environments depending on your infrastructure and workflow.
This guide covers working examples for each environment a data engineer typically encounters.

## Environments at a glance

| Environment | Use Case | Master URL | Best for |
|-------------|----------|------------|----------|
| [Local](environments/local.md) | Development & unit testing | `local[*]` | Writing & debugging jobs |
| [Shell](environments/shell.md) | Interactive exploration | `pyspark` CLI | Quick ad-hoc queries |
| [spark-submit](environments/spark-submit.md) | Batch jobs | `local` / cluster URL | Scheduled production jobs |
| [Notebook](environments/notebook.md) | Exploratory data analysis | Jupyter + PySpark | EDA & prototyping |
| [YARN](cluster/yarn.md) | On-premise Hadoop cluster | `yarn` | Existing Hadoop estates |
| [Kubernetes](cluster/kubernetes.md) | Cloud-native / containerised | `k8s://<api-server>` | Modern cloud deployments |
| [EMR on EC2](aws/emr.md) | AWS managed Spark on EC2 | YARN (managed by EMR) | AWS on-demand clusters |
| [EMR Serverless](aws/emr-serverless.md) | Serverless Spark on AWS | Managed by AWS | Intermittent batch jobs |
| [AWS Glue](aws/glue.md) | Serverless ETL with Catalog | Managed by Glue | Catalog-driven ETL |

## Quick Start

!!! tip "No cluster needed"
    Start with local mode — it runs on your laptop in seconds.

```bash
pip install pyspark
python local/local_example.py
```

## Common Pattern

Every PySpark job starts with a `SparkSession`. Only `.master()` and a few
config keys change between environments:

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("my-job")
         .master("local[*]")  # (1)!
         .getOrCreate())
```

1. Replace with `yarn`, `k8s://...`, etc. for cluster environments — or omit
   entirely and let `spark-submit --master` inject it at runtime.

## Choosing an Environment

```
New feature / debugging?
     └─► Local

Need to explore data interactively?
     ├─► Shell        (command line)
     └─► Notebook     (visual / shareable)

Running a scheduled batch job?
     ├─► spark-submit  (any master)
     ├─► YARN          (on-premise Hadoop)
     └─► Kubernetes    (cloud / containerised)

Running on AWS?
     ├─► EMR on EC2       (managed cluster, SSH access, YARN)
     ├─► EMR Serverless   (no cluster to manage, pay-per-second)
     └─► AWS Glue         (serverless + Glue Data Catalog + DynamicFrame)
```
