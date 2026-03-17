# Running PySpark on Different Environments

PySpark can run in several modes depending on your infrastructure. This tutorial covers
working examples for each environment a data engineer typically encounters.

## Environments

| Mode | Use Case | Master URL |
|------|----------|------------|
| [local/](local/) | Development & unit testing | `local[*]` |
| [shell/](shell/) | Interactive exploration | `pyspark` CLI |
| [spark-submit/](spark-submit/) | Batch jobs via `spark-submit` | `local` / cluster URL |
| [notebook/](notebook/) | Exploratory data analysis | Jupyter + PySpark |
| [cluster/yarn/](cluster/yarn/) | On-premise Hadoop cluster | `yarn` |
| [cluster/kubernetes/](cluster/kubernetes/) | Cloud-native / containerised | `k8s://<api-server>` |

## Quick Start

```bash
# Install PySpark locally
pip install pyspark

# Run the local example immediately — no cluster needed
python mode/local/local_example.py
```

## Common Pattern

Every PySpark job starts with a `SparkSession`. Only the `.master()` and a few
config options change between environments:

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("my-job")
         .master("local[*]")   # swap this per environment
         .getOrCreate())
```
