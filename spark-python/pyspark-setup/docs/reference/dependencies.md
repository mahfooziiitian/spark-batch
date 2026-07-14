# Dependencies

## Core (always installed with PySpark)

| Package | Min version | Role |
|---------|-------------|------|
| `py4j` | 0.10.9.7 | **Required** — bridges Python ↔ JVM |
| `pyspark` | 3.5.0 | Core Spark Python API |

## Data Stack (strongly recommended)

| Package | Min version | Role |
|---------|-------------|------|
| `pyarrow` | 4.0.0 | **Required** for pandas API on Spark; Arrow-based columnar I/O |
| `pandas` | 1.0.5 | **Required** for pandas API on Spark (`spark.createDataFrame(pdf)`) |
| `numpy` | 1.15 | **Required** for MLlib DataFrame-based API and UDFs |

## Optional

| Package | Role |
|---------|------|
| `findspark` | Locates Spark when `SPARK_HOME` is not set |
| `venv-pack` | Packs a virtualenv into a `.tar.gz` for YARN `--archives` |
| `aws-glue-sessions` | Local Glue library emulation for unit testing |
| `pytest` | Test framework used in `ci/test_pyspark.py` |
| `jupyter` / `ipykernel` | Notebook support |

## `local/requirements.txt`

```text
--8<-- "local/requirements.txt"
```

## `conda/environment.yml`

```yaml
--8<-- "conda/environment.yml"
```

## `pyproject.toml`

```toml
--8<-- "pyproject.toml"
```

## Hadoop Version Variants (pip)

Control which Hadoop flavour is bundled:

```bash
# Default — Hadoop 3.3+ (recommended)
pip install pyspark==3.5.0

# Hadoop 2.7 legacy
PYSPARK_HADOOP_VERSION=2 pip install pyspark==3.5.0

# No bundled Hadoop (use cluster's own)
PYSPARK_HADOOP_VERSION=without pip install pyspark==3.5.0
```
