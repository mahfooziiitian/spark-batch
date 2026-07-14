---
applyTo: "**/*.py"
---

# GCS Storage Instructions

## Protocol

Use `gs://` for all Google Cloud Storage access.

## Path Format

```
gs://<BUCKET>/<PATH>
```

## JARs

The GCS connector is not on Maven Central. Load it via local path:

```python
.config("spark.jars", "/path/to/gcs-connector-hadoop3-latest.jar")
```

Always set the filesystem implementation:

```python
.config("spark.hadoop.fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
```

## Authentication Methods

### Service Account Key File

```python
.config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
.config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
        keyfile_path)
```

### Application Default Credentials (ADC)

```python
.config("spark.hadoop.google.cloud.auth.service.account.enable", "false")
```

Requires `gcloud auth application-default login` beforehand.

### Workload Identity (GKE)

No extra Spark config needed — the GKE metadata server provides credentials.

## Environment Variables

```bash
GOOGLE_APPLICATION_CREDENTIALS    # path to service account JSON keyfile
GCS_CONNECTOR_JAR                 # path to the GCS connector JAR
```

## Dataproc Note

On Google Cloud Dataproc the GCS connector is pre-installed. No extra JARs or
auth config required.
