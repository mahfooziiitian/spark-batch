# PySpark Setup

Install and configure PySpark on any environment — from a developer laptop to a
production Kubernetes cluster.

## Environments at a Glance

| Environment | Guide | Best for |
|-------------|-------|----------|
| [Local / venv](environments/local.md) | Isolated Python virtualenv | Day-to-day development |
| [Conda](environments/conda.md) | Anaconda / Miniconda | Data science workflows |
| [Docker](environments/docker.md) | Containerised development | Reproducible environments |
| [YARN](cluster/yarn.md) | On-premise Hadoop cluster | Existing Hadoop estates |
| [Kubernetes](cluster/kubernetes.md) | Cloud-native | Modern cloud deployments |
| [AWS EMR](aws/emr.md) | Managed Spark on EC2 | AWS on-demand clusters |
| [AWS Glue](aws/glue.md) | Serverless ETL | Catalog-driven pipelines |
| [CI/CD](ci.md) | GitHub Actions | Automated testing |

## Quick Start

!!! tip "No cluster needed"
    Install PySpark locally in under two minutes.

=== "pip"
    ```bash
    pip install "pyspark==3.5.0"
    python -c "import pyspark; print(pyspark.__version__)"
    ```

=== "conda"
    ```bash
    conda install -c conda-forge pyspark=3.5.0
    ```

=== "uv"
    ```bash
    uv add "pyspark==3.5.0"
    ```

## Prerequisites

!!! warning "Java required"
    PySpark requires **Java 8, 11, or 17**. Java 11 LTS is recommended.

    ```bash
    java -version   # must print a version before you can use PySpark
    ```

## Version Compatibility

| PySpark | Python       | Java         | Scala           |
|---------|--------------|--------------|-----------------|
| 3.5.x   | 3.8 – 3.12   | 8 / 11 / 17  | 2.12 / 2.13     |
| 3.4.x   | 3.7 – 3.11   | 8 / 11 / 17  | 2.12 / 2.13     |
| 3.3.x   | 3.7 – 3.10   | 8 / 11       | 2.12 / 2.13     |
| 2.4.x   | 2.7+ / 3.4+  | 8            | 2.12            |

## Common SparkSession Pattern

Every example in this guide uses the same environment-agnostic pattern —
only the `.master()` and a few config keys differ between environments:

```python
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("my-job")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))  # (1)!
         .config("spark.sql.adaptive.enabled", "true")        # (2)!
         .getOrCreate())
```

1. Reads the master URL from `SPARK_MASTER` env var; falls back to `local[*]`
   so the same script runs on a laptop and on a cluster without code changes.
2. Adaptive Query Execution — auto-tunes shuffle partitions and skew joins.
