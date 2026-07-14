# GitHub Copilot Instructions — PySpark MongoDB Tutorial

## Project Overview

A **PySpark + MongoDB** tutorial project demonstrating how to read from and write to
MongoDB using the Spark MongoDB Connector. The project includes Python examples,
Docker-based MongoDB infrastructure, and MkDocs Material documentation.

## Repository Layout

```
pyspark-monogdb/
├── .github/
│   ├── copilot-instructions.md          # Global instructions (this file)
│   └── instructions/
│       ├── pyspark.instructions.md      # Python / PySpark code
│       ├── testing.instructions.md      # pytest test files
│       ├── docs.instructions.md         # MkDocs Material documentation
│       ├── infra.instructions.md        # Docker Compose & Dockerfiles
│       └── shell.instructions.md        # Bash scripts
├── infra/
│   └── docker/
│       └── docker-compose.yml           # MongoDB + Mongo Express
├── src/
│   └── mongondb/
│       └── mongodb_collection.py        # PySpark ↔ MongoDB examples
├── docs/                                # MkDocs Material documentation
├── pyproject.toml                       # uv project config
├── requirements.txt
├── uv.lock
└── README.md
```

## Tech Stack & Versions

| Component                | Version                                      |
| ------------------------ | -------------------------------------------- |
| Python                   | 3.11                                         |
| Apache Spark / PySpark   | 3.5.x (< 4.0.0)                             |
| MongoDB Spark Connector  | `mongo-spark-connector_2.13:10.1.1`          |
| MongoDB (Docker)         | 5.0.17                                       |
| Mongo Express (Docker)   | latest                                       |
| Documentation            | MkDocs Material ≥ 9.7                        |
| Package management       | uv                                           |
| Task runner              | taskipy                                      |
| Java                     | 11 (LTS)                                     |

## General Conventions

- **No boilerplate comments.** Only comment code that needs clarification.
- Use `uv` for dependency management; `pip` only as a fallback.
- Parquet is the preferred output format unless MongoDB is the target.
- Default credentials in Docker are `mongo` / `mongo` — dev only, never production.

## Environment Variables

| Variable         | Default                      | Description                      |
| ---------------- | ---------------------------- | -------------------------------- |
| `SPARK_MASTER`   | `local[*]`                   | Spark master URL                 |
| `MONGO_URI`      | `mongodb://127.0.0.1:27017`  | MongoDB connection string        |
| `MONGO_DB`       | `tutorial`                   | Default MongoDB database         |
| `JAVA_HOME`      | _(system)_                   | Path to Java 11 installation     |
| `PYSPARK_PYTHON` | `python3`                    | Python binary for PySpark workers|

## MongoDB Spark Connector Configuration Reference

| Config key                          | Description                         | Example value                                         |
| ----------------------------------- | ----------------------------------- | ----------------------------------------------------- |
| `spark.jars.packages`               | Maven coordinates for the connector | `org.mongodb.spark:mongo-spark-connector_2.13:10.1.1` |
| `spark.mongodb.read.connection.uri` | MongoDB connection URI for reads    | `mongodb://127.0.0.1:27017`                           |
| `spark.mongodb.write.connection.uri`| MongoDB connection URI for writes   | `mongodb://127.0.0.1:27017`                           |
| `.option("database", ...)`          | Target database name                | `tutorial`                                            |
| `.option("collection", ...)`        | Target collection name              | `people`                                              |

## Quick Start

```bash
# 1. Start MongoDB
cd infra/docker && docker compose up -d && cd -

# 2. Install dependencies
uv sync

# 3. Run example
uv run python src/mongondb/mongodb_collection.py

# 4. Verify in Mongo Express
open http://localhost:8081
```
