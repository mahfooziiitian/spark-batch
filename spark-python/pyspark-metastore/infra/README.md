# Infrastructure Setup

Setup scripts and Docker Compose configurations for each metastore/catalog type.

## Directory Structure

| Directory | Catalog Type | Dependencies |
|-----------|-------------|--------------|
| `common/` | Shared assets | Dockerfile, .env template, health checks |
| `local/` | In-Memory / Spark Built-in | Java, Python only |
| `hive/` | Hive Metastore | PostgreSQL + Hive Metastore Service |
| `external-rdbms/` | External RDBMS | PostgreSQL or MySQL |
| `jdbc/` | JDBC Catalog | PostgreSQL with sample data |
| `iceberg/` | Iceberg (REST) | Iceberg REST Catalog + MinIO |
| `delta-lake/` | Delta Lake | Spark with Delta JARs |
| `rest/` | REST Catalog | Iceberg REST Catalog |
| `hadoop/` | Hadoop Catalog | HDFS (NameNode + DataNode) |
| `glue/` | AWS Glue | AWS credentials + LocalStack |
| `unity-catalog/` | Databricks UC | Databricks workspace |

## Quick Start

### Local (no Docker required)
```bash
bash infra/local/setup.sh
```

### Hive Metastore
```bash
cd infra/hive
docker compose up -d
bash ../common/health-check.sh hive-metastore
```

### Iceberg REST + MinIO
```bash
cd infra/iceberg
docker compose up -d
bash ../common/health-check.sh iceberg-rest
```

## Common Commands

```bash
# Health check all services
bash infra/common/health-check.sh all

# Smoke test Spark
bash infra/common/smoke-test.sh

# Stop all services
docker compose -f infra/hive/docker-compose.yml down
docker compose -f infra/iceberg/docker-compose.yml down
```
