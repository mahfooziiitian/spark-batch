# PySpark Standalone Cluster — Docker Compose

Spin up a complete PySpark standalone cluster on your laptop with a single command.

## Services

| Service | Description | URL |
|---------|-------------|-----|
| `spark-master` | Standalone master | <http://localhost:8080> |
| `spark-worker` | Workers (2 by default) | registered on master UI |
| `spark-history` | History server — browse completed jobs | <http://localhost:18080> |
| `notebook` | JupyterLab (token: `spark`) | <http://localhost:8888> |
| `spark-submit` | One-shot job runner (profile) | — |

## Quick Start

```bash
# 1. Start the cluster
docker compose up -d

# 2. Wait ~30 s for the master health check, then open the Spark UI
open http://localhost:8080      # macOS
xdg-open http://localhost:8080  # Linux

# 3. Open JupyterLab  (token: spark)
open http://localhost:8888
```

Or use the Makefile:

```bash
make up          # start cluster
make ps          # check services
make ui-master   # open Spark UI
make ui-notebook # open JupyterLab
```

## Submit a Job

```bash
# Submit the default ETL job (jobs/etl_job.py)
docker compose --profile submit run --rm spark-submit

# Submit a custom job
JOB_FILE=etl_job.py docker compose --profile submit run --rm spark-submit

# Via Makefile
make submit
make submit-job JOB_FILE=etl_job.py
```

## Scale Workers

```bash
# Add more workers at runtime
docker compose up -d --scale spark-worker=4

# Or set the default in .env
echo "SPARK_WORKER_REPLICAS=4" >> .env
docker compose up -d

# Via Makefile
make scale WORKERS=4
```

## Notebooks

The `notebooks/` directory is mounted at `/notebooks` inside the JupyterLab container.

| Notebook | Description |
|----------|-------------|
| `word_count.py` | Word-frequency analysis connecting to the cluster |
| `cluster_example.py` | Web analytics — funnel, running totals, engagement ranking |

Run from the JupyterLab terminal:

```bash
python word_count.py
python cluster_example.py
```

## Configuration

Edit `.env` to change defaults:

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARK_WORKER_REPLICAS` | `2` | Number of worker containers |
| `SPARK_WORKER_MEMORY` | `2g` | Memory per worker |
| `SPARK_WORKER_CORES` | `2` | CPU cores per worker |
| `SPARK_MASTER_UI_PORT` | `8080` | Master web UI port |
| `SPARK_HISTORY_PORT` | `18080` | History server port |
| `JUPYTER_PORT` | `8888` | JupyterLab port |
| `JUPYTER_TOKEN` | `spark` | JupyterLab access token |

Spark properties shared across all services are in [`conf/spark-defaults.conf`](conf/spark-defaults.conf).

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Docker network: spark-net                               │
│                                                          │
│  spark-master :7077 :8080                                │
│       │                                                  │
│       ├──► spark-worker-1  (2 g / 2 cores)               │
│       ├──► spark-worker-2  (2 g / 2 cores)               │
│       │                                                  │
│       ├──► spark-history :18080  (reads spark-events/)   │
│       │                                                  │
│       └──► notebook :8888  (JupyterLab)                  │
│                                                          │
│  Volume: spark-events  (event logs, shared)              │
└──────────────────────────────────────────────────────────┘
```

## Stop & Clean Up

```bash
# Stop containers, keep volumes
docker compose down

# Full reset (removes volumes / event logs)
docker compose down -v

# Via Makefile
make down     # stop
make clean    # stop + remove volumes
```
