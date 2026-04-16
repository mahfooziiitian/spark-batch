# PySpark Storage

PySpark examples for reading and writing data to **cloud and local object storage** systems
using Apache Spark 3.5.x.

Each storage backend lives in its own sub-project with self-contained examples,
authentication patterns, infrastructure-as-code, and documentation.

## Sub-Projects

| Sub-Project | Storage | Protocol | Infra |
|-------------|---------|----------|-------|
| [AWS S3](aws-s3/) | Amazon S3 | `s3a://` | Terraform |
| [Azure Storage](azure-storage/) | Azure Blob / ADLS Gen2 | `abfss://` | Terraform |
| [GCP Storage](gcp-storage/) | Google Cloud Storage | `gs://` | Terraform |
| [HDFS](HDFS/) | Hadoop Distributed File System | `hdfs://` | Docker Compose |
| [LocalStack S3](localstack-s3/) | S3 (LocalStack) | `s3a://` | Docker Compose |
| [MinIO](MinIO/) | MinIO (S3-compatible) | `s3a://` | Docker Compose |

## Quick Start

```bash
# Clone and enter the project
cd pyspark-storage

# Pick a provider (e.g. LocalStack for local dev)
cd pyspark-local-s3

# Stand up infrastructure
./setup.sh

# Install Python deps
uv sync

# Run an example
python src/s3a/read_s3_spark_config.py

# Tear down when done
./teardown.sh
```

## Common Patterns

All sub-projects follow the same conventions:

- **Spark config approach** — credentials via `spark.hadoop.fs.*` properties in SparkSession builder
- **Hadoop config approach** — credentials via `sc._jsc.hadoopConfiguration().set(...)` at runtime
- **Environment variables** — `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` or provider equivalent
- **JAR dependencies** — managed through `spark.jars.packages` config

## Project Layout

Every sub-project follows this structure:

```
pyspark-<provider>/
├── pyproject.toml               # uv project (PEP 621)
├── README.md                    # Provider overview
├── setup.sh                     # Provision infra + upload sample data
├── teardown.sh                  # Destroy infra
├── docker-compose.yml           # (local providers) or infra/ (cloud providers)
├── data/
│   └── sample.csv               # Sample dataset
├── src/<protocol>/
│   ├── read_<target>_spark_config.py
│   ├── read_<target>_hadoop_config.py
│   ├── write_<target>_parquet.py
│   └── authentication/
│       └── spark_<provider>_<method>.py
└── docs/
    └── index.md                 # MkDocs documentation
```

## Tech Stack

| Component | Version |
|-----------|---------|
| Apache Spark / PySpark | 3.5.x |
| Python | 3.11+ |
| Java | 11 (LTS) |
| Package manager | uv |
| Documentation | MkDocs Material |
| IaC (cloud) | Terraform |
| IaC (local) | Docker Compose |
