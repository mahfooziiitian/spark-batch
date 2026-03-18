# Getting Started

This guide walks you through setting up the environment and running your first
PySpark ↔ MongoDB pipeline.

## Prerequisites

!!! warning "Java required"
    Java 11 must be on your `PATH`. The MongoDB Spark Connector requires a JVM.

    ```bash
    java -version   # should print 11.x
    ```

### Python & Dependencies

=== "uv (Recommended)"
    ```bash
    uv sync
    ```

=== "pip"
    ```bash
    pip install -r requirements.txt
    ```

### MongoDB

Start the Docker stack (MongoDB 5.0 + Mongo Express):

```bash
cd infra/docker
docker compose up -d
cd -
```

Verify MongoDB is running:

```bash
docker compose -f infra/docker/docker-compose.yml ps
```

| Service       | URL                     | Credentials     |
| ------------- | ----------------------- | --------------- |
| MongoDB       | `localhost:27017`       | `mongo` / `mongo` |
| Mongo Express | `http://localhost:8081` | `mongo` / `mongo` |

## Environment Variables

All scripts use environment variables with sensible defaults — no changes needed
for local development:

| Variable       | Default                                  | Description                  |
| -------------- | ---------------------------------------- | ---------------------------- |
| `SPARK_MASTER` | `local[*]`                               | Spark master URL             |
| `MONGO_URI`    | `mongodb://mongo:mongo@127.0.0.1:27017`  | MongoDB connection string    |
| `MONGO_DB`     | `tutorial`                               | Default MongoDB database     |
| `JAVA_HOME`    | _(system)_                               | Path to Java 11 installation |

## Run Your First Example

```bash
uv run python src/mongondb/mongodb_collection.py
```

You should see a schema printout, the DataFrame contents, and a confirmation that
data was written and read back from MongoDB.

!!! tip "Verify in Mongo Express"
    Open [http://localhost:8081](http://localhost:8081) and navigate to the
    `tutorial` database to see the `people` and `elders` collections.

## SparkSession Configuration

Every script creates a SparkSession with the MongoDB connector:

```python
spark = (
    SparkSession.builder
    .appName("mongodb-collection")
    .master(os.environ.get("SPARK_MASTER", "local[*]"))  # (1)!
    .config(
        "spark.jars.packages",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0",  # (2)!
    )
    .config("spark.mongodb.read.connection.uri", MONGO_URI)   # (3)!
    .config("spark.mongodb.write.connection.uri", MONGO_URI)
    .getOrCreate()
)
```

1. Falls back to local mode when no cluster is configured.
2. Maven coordinates — Spark downloads the JAR automatically on first run.
3. V10 connector config keys (`read`/`write`), not the deprecated V1 `input`/`output` keys.

## Next Steps

- [Collections example](examples/collections.md) — write, read, filter
- [Aggregations example](examples/aggregations.md) — groupBy, window functions, rankings
- [Infrastructure details](infrastructure/index.md) — Docker Compose deep dive
- [Configuration reference](configuration.md) — all Spark + MongoDB config keys
