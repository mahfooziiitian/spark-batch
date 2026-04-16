---
applyTo: "**"
---

# PySpark Storage Access Instructions

## Project Structure

Each storage sub-project follows this layout:

```
pyspark-<provider>/
├── .github/instructions/       # Provider-specific Copilot instructions
├── pyproject.toml              # uv project (PEP 621)
├── README.md                   # Provider overview, setup, auth methods
├── src/
│   └── <protocol>/             # s3a, abfss, gs, hdfs
│       ├── read_<provider>_spark_config.py
│       ├── read_<provider>_hadoop_config.py
│       └── authentication/
│           └── spark_<provider>_<method>.py
└── tests/
    └── test_<provider>.py      # pytest + pytest-mock tests
```

## pyproject.toml (uv)

All sub-projects use PEP 621 format with uv. Do **not** use Poetry.

```toml
[project]
name = "pyspark-<provider>"
version = "0.1.0"
description = "PySpark <Provider> storage examples"
readme = "README.md"
requires-python = ">=3.11"
dependencies = [
    "pyspark>=3.5.2",
]

[dependency-groups]
dev = [
    "pytest>=8.0",
    "pytest-mock>=3.14",
]
```

## Naming Conventions

| Item | Pattern | Example |
|------|---------|---------|
| Sub-project directory | `pyspark-<provider>` | `pyspark-aws-s3` |
| Source directory | `src/<protocol>` | `src/s3a`, `src/abfss`, `src/gs` |
| Spark config script | `read_<target>_spark_config.py` | `read_s3_spark_config.py` |
| Hadoop config script | `read_<target>_hadoop_config.py` | `read_s3_hadoop_config.py` |
| Auth script | `spark_<provider>_<method>.py` | `spark_s3_env_auth.py` |
| Test file | `test_<provider>.py` | `test_aws_s3.py` |

## Environment Variables

Provider credentials:

| Provider   | Key Env Var              | Secret Env Var                |
|------------|--------------------------|-------------------------------|
| AWS S3     | `AWS_ACCESS_KEY_ID`      | `AWS_SECRET_ACCESS_KEY`       |
| Azure      | `AZURE_STORAGE_ACCOUNT`  | `AZURE_STORAGE_KEY`           |
| GCP        | `GOOGLE_APPLICATION_CREDENTIALS` | (path to JSON keyfile) |
| HDFS       | (none — uses OS user or Kerberos) | `KRB5_KTNAME`        |
| MinIO      | `MINIO_ACCESS_KEY`       | `MINIO_SECRET_KEY`            |
| LocalStack | `AWS_ACCESS_KEY_ID`      | `AWS_SECRET_ACCESS_KEY`       |

Common env vars across all sub-projects:

```bash
SPARK_MASTER     # Spark master URL (default: local[*])
INPUT_PATH       # Input data path
OUTPUT_PATH      # Output data path
```

## Adding a New Storage Provider

1. Create `pyspark-<provider>/` under `pyspark-storage/`.
2. Add `.github/instructions/` with provider-specific instructions.
3. Add `pyproject.toml` following the uv template above.
4. Add `README.md` following the README structure.
5. Add `src/<protocol>/` with Spark config and Hadoop config scripts.
6. Add `src/<protocol>/authentication/` with one script per auth method.
7. Add `tests/test_<provider>.py` using pytest and pytest-mock.
8. Update the root `pyspark-storage/README.md` sub-projects table.
