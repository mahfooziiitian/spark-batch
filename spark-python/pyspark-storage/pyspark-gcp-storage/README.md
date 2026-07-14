# Google Cloud Storage and Spark

PySpark examples for reading and writing data to **Google Cloud Storage (GCS)**
using the `gs://` protocol.

## Prerequisites

- Java 11
- PySpark 3.5.x
- GCP service account credentials (JSON key file)

## Library

GCS access depends on the **GCS connector** JAR:

- `gcs-connector` (maintained by Google as `hadoop3-x.y.z-shaded.jar`)

Download from [GoogleCloudDataproc/hadoop-connectors](https://github.com/GoogleCloudDataproc/hadoop-connectors).

## Authentication Methods

### Service Account Key File

```python
spark = (SparkSession.builder
         .appName("gcs-demo")
         .config("spark.jars", "/path/to/gcs-connector-hadoop3-latest.jar")
         .config("spark.hadoop.fs.gs.impl",
                 "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
         .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
         .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                 "/path/to/keyfile.json")
         .getOrCreate())
```

### Application Default Credentials

```bash
gcloud auth application-default login
```

```python
.config("spark.hadoop.google.cloud.auth.service.account.enable", "false")
```

### Workload Identity (GKE)

No extra Spark config needed — credentials are provided by the GKE metadata server
when the Kubernetes service account is annotated with the GCP service account.

## Path Format

```
gs://<BUCKET>/<PATH>
```

## Dataproc Note

On Google Cloud Dataproc the GCS connector is pre-installed. No extra JARs or
authentication config required — it uses the cluster's service account automatically.
