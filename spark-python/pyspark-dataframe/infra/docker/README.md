# Docker — PySpark DataFrame

Containerised development environment for running PySpark examples, tests, and
JupyterLab notebooks.

## Architecture

```text
┌─────────────┐      ┌──────────────┐      ┌────────────┐
│ spark-master │◄────►│ spark-worker │ × N  │   MySQL    │
│   :8080 UI   │      │              │      │   :3306    │
└──────┬───────┘      └──────────────┘      └─────┬──────┘
       │                                          │
       │          ┌────────────────┐               │
       └─────────►│  pyspark-app   │◄──────────────┘
                  │  (scripts/tests)│
                  └────────────────┘
                  ┌────────────────┐
                  │   JupyterLab   │
                  │     :8888      │
                  └────────────────┘
```

## Prerequisites

- Docker Engine ≥ 20.10
- Docker Compose v2 (the `docker compose` plugin)

## Quick Start

```bash
cd infra/docker

# Start the full stack
make up

# Run a specific script
make run S=src/data_frame/transformation/filter/filter_operations.py

# Run tests
make test

# Open JupyterLab → http://localhost:8888
make jupyter

# Stop everything
make down
```

## Configuration

All tunables live in **`.env`** (same directory). Copy it to `.env.local` and
edit for machine-specific overrides.

| Variable | Default | Description |
|---|---|---|
| `SPARK_VERSION` | `3.5.0` | Apache Spark image tag |
| `PY4J_VERSION` | `0.10.9.7` | py4j version bundled with the Spark image |
| `SPARK_WORKER_CORES` | `2` | CPU cores per worker |
| `SPARK_WORKER_MEMORY` | `2g` | Memory per worker |
| `MYSQL_PORT` | `3306` | Host port for MySQL |
| `MYSQL_USER` | `spark_user` | MySQL application user |
| `MYSQL_PASSWORD` | `Spark_2024` | MySQL application password |
| `JUPYTER_PORT` | `8888` | Host port for JupyterLab |
| `JUPYTER_TOKEN` | *(empty)* | JupyterLab auth token |
| `CONFIG_PROFILE` | `dev` | Maps to `configs/dev.yaml` |

## Dockerfile

A single **multi-stage** Dockerfile with three stages:

| Stage | Purpose | Produced by |
|---|---|---|
| `base` | Spark + pip deps + env vars | shared layer |
| `app` | Project code + entrypoint | `docker compose build pyspark-app` |
| `jupyter` | JupyterLab + viz libraries | `docker compose build jupyter` |

```bash
# Build just the app image
make build-app

# Build just the jupyter image
make build-jupyter

# Build both
make build
```

## Entrypoint Commands

The `entrypoint.sh` accepts a subcommand as the first argument:

```bash
docker compose run --rm pyspark-app run    src/data_frame/etl/etl.py
docker compose run --rm pyspark-app submit src/data_frame/etl/etl.py
docker compose run --rm pyspark-app test   tests/ -v
docker compose run --rm pyspark-app shell
docker compose run --rm pyspark-app wait mysql 3306 30 -- run src/data_frame/io/...
docker compose run --rm pyspark-app sleep
docker compose run --rm pyspark-app bash
```

## Scaling Workers

```bash
make scale N=4          # 4 workers
make spark-cluster      # 1 master + 1 worker
```

## Dev Mode (Hot-Reload)

The `docker-compose.override.yml` is active by default and volume-mounts
`src/` and `configs/` as read-only into both `pyspark-app` and `jupyter`.
Edit files on the host and re-run — no rebuild needed.

To disable the override:

```bash
docker compose -f docker-compose.yml up -d
```

## MySQL Seed Data

On first start, MySQL executes `init-sql/init.sql` which creates tables matching
the project's `sample_data.py` datasets: `employees`, `departments`, `salaries`,
`customer_orders`.

## Cleanup

```bash
make down          # stop containers
make clean         # stop + remove volumes (MySQL data, Jupyter state)
```
