# Configuration Reference

All Spark and MongoDB Connector configuration keys used in this project.

## Spark MongoDB Connector

| Config Key                            | Description                         | Example Value                                                 |
| ------------------------------------- | ----------------------------------- | ------------------------------------------------------------- |
| `spark.jars.packages`                 | Maven coordinates for the connector | `org.mongodb.spark:mongo-spark-connector_2.12:10.4.0`         |
| `spark.mongodb.read.connection.uri`   | MongoDB connection URI for reads    | `mongodb://mongo:mongo@127.0.0.1:27017`                       |
| `spark.mongodb.write.connection.uri`  | MongoDB connection URI for writes   | `mongodb://mongo:mongo@127.0.0.1:27017`                       |

!!! note "V10 connector config keys"
    The V10 connector uses `spark.mongodb.read.*` / `spark.mongodb.write.*` keys.
    The legacy `spark.mongodb.input.*` / `spark.mongodb.output.*` keys are
    deprecated and only work with the V1 connector.

## Read/Write Options

These are set per operation via `.option()`:

| Option       | Description            | Example  |
| ------------ | ---------------------- | -------- |
| `database`   | Target database name   | `tutorial` |
| `collection` | Target collection name | `people` |

## Write Modes

| Mode        | Behaviour                                                    |
| ----------- | ------------------------------------------------------------ |
| `overwrite` | Drops the collection and writes fresh data                   |
| `append`    | Adds documents to the existing collection                    |

## SparkSession Configuration

| Config Key                                       | Value      | Description                          |
| ------------------------------------------------ | ---------- | ------------------------------------ |
| `spark.master`                                   | `local[*]` | Use all CPU cores locally            |
| `spark.sql.adaptive.enabled`                     | `true`     | Enable Adaptive Query Execution      |
| `spark.sql.adaptive.coalescePartitions.enabled`  | `true`     | Auto-coalesce small partitions       |
| `spark.sql.shuffle.partitions`                   | `4`        | Reduce from default 200 for local    |
| `spark.ui.enabled`                               | `false`    | Disable Spark Web UI in tests        |

## Environment Variables

| Variable         | Default                                  | Description                      |
| ---------------- | ---------------------------------------- | -------------------------------- |
| `SPARK_MASTER`   | `local[*]`                               | Spark master URL                 |
| `MONGO_URI`      | `mongodb://mongo:mongo@127.0.0.1:27017`  | MongoDB connection string        |
| `MONGO_DB`       | `tutorial`                               | Default MongoDB database         |
| `JAVA_HOME`      | _(system)_                               | Path to Java 11 installation     |
| `PYSPARK_PYTHON` | `python3`                                | Python binary for PySpark workers|

## Connector Compatibility

| Connector Version | Spark Versions | Scala |
| ----------------- | -------------- | ----- |
| 10.4.0            | 3.5.x          | 2.12  |
| 10.3.x            | 3.4.x – 3.5.x  | 2.12 / 2.13 |
| 10.2.x            | 3.2.x – 3.4.x  | 2.12 / 2.13 |
| 10.1.x            | 3.1.x – 3.3.x  | 2.12 / 2.13 |

!!! warning "Scala version must match"
    PySpark from PyPI ships with Scala 2.12 JARs. Always use the `_2.12`
    connector variant. Using `_2.13` causes `NoSuchMethodError` at runtime.

!!! success "Good fit"
    - Batch ETL from/to MongoDB
    - Aggregation pipelines too complex for the MongoDB aggregation framework
    - Joining MongoDB data with other Spark data sources

!!! failure "Not a good fit"
    - Real-time streaming (use Kafka + Structured Streaming instead)
    - Sub-second latency queries (query MongoDB directly)
    - Very small datasets (use PyMongo directly — no Spark overhead)
