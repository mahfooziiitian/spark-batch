# Copilot Instructions — pyspark-kafka

## Project Overview

This project demonstrates **Apache Kafka integration with PySpark**, covering batch reading, batch producing, and structured streaming patterns. It runs against a local **3-node Kafka cluster** provisioned via Docker Compose using Confluent Platform images.

## Tech Stack

| Component | Details |
|-----------|---------|
| Language | Python ≥ 3.11 |
| Spark | PySpark 3.5.1 (`local[*]` master) |
| Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` (loaded via `spark.jars.packages`) |
| MySQL connector | `com.mysql:mysql-connector-j:8.0.32` (for offset tracking) |
| Cluster | 3 ZooKeeper + 3 Kafka brokers (Confluent `cp-zookeeper` / `cp-kafka`) |
| Monitoring | Kafka-UI (port 8080), Kafdrop (port 9000) |
| Package config | `pyproject.toml` using `[project]` table format (uv/pip compatible, no Poetry) |

## Project Structure

```text
src/
├── pyspark_kafka.py              # Batch read from Kafka topic
├── pyspark_producer.py           # Write JSON data to Kafka topic via Spark
├── util/
│   └── config_reader.py          # ConfigParser wrapper for JDBC credentials
└── streaming/
    ├── pyspark_streaming.py      # Full structured streaming pipeline: readStream → foreachBatch → JSON + MySQL offsets
    ├── listener/                 # StreamingQueryListener implementations
    ├── headers/                  # Kafka headers access
    ├── fault_tolerance/          # Checkpoint and fault tolerance patterns
    ├── termination/              # Graceful stream termination
    ├── operation_sdf/            # Streaming DataFrame operations
    ├── aggregation/              # Streaming aggregations
    ├── result_table/             # Result table patterns
    ├── query/                    # Event-time window-based aggregations
    ├── input_table/              # Input table patterns
    ├── schema/                   # Schema inference and struct schema for streaming
    ├── water_marking/            # Watermark handling for late data
    └── partition_discovery/      # Partition discovery
docker-compose.yml                # 3-node Kafka cluster with ZooKeeper ensemble
pyproject.toml                    # Project metadata ([project] table format)
```

## Modular Instruction Files

| File | Scope | Purpose |
|------|-------|---------|
| `instructions/python.instructions.md` | `**/*.py` | Python coding style and conventions |
| `instructions/pyspark-kafka.instructions.md` | `src/**/*.py` | Kafka + Spark integration patterns |
| `instructions/testing.instructions.md` | `tests/**/*.py` | pytest conventions and Spark testing |
| `instructions/docker.instructions.md` | `**/Dockerfile`, `**/docker-compose*.yml` | Docker and Kafka cluster configuration |
| `instructions/project-config.instructions.md` | `pyproject.toml` | Package and project configuration |

## Quick Reference

- **Bootstrap servers:** `localhost:19091,localhost:29091,localhost:39091`
- **Start cluster:** `docker compose up -d` (requires `confluent_version` env var)
- **Kafka-UI:** `http://localhost:8080`
- **Kafdrop:** `http://localhost:9000`
- **Run batch read:** `python src/pyspark_kafka.py`
- **Run producer:** `python src/pyspark_producer.py` (requires `DATA_HOME` env var)
- **Run streaming:** `python src/streaming/pyspark_streaming.py` (requires `DATA_HOME` env var)

## Things to Avoid

- Do not hardcode JDBC credentials — use `ConfigReader` with external config files.
- Do not embed Kafka broker addresses in library code; keep them in top-level scripts or configuration.
- Do not use `spark.read` (batch) when the intent is streaming; use `spark.readStream`.
- Do not skip `CAST(key AS STRING)` / `CAST(value AS STRING)` — Kafka columns are binary by default.
- Do not omit checkpoint locations for streaming queries that require fault tolerance.
- Do not commit `.env` files containing `confluent_version` or database credentials.
