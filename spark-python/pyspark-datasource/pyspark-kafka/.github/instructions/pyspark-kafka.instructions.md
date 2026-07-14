---
applyTo: "src/**/*.py"
---

# PySpark Kafka Integration Patterns

## SparkSession Setup

- Run in `local[*]` mode for development.
- Load Kafka and MySQL connector JARs via `spark.jars.packages`:
  ```python
  .config("spark.jars.packages", ",".join([
      "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
      "com.mysql:mysql-connector-j:8.0.32"
  ]))
  ```
- Set `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to `sys.executable` to avoid interpreter mismatches.

## Bootstrap Servers

- The local 3-broker cluster exposes: `localhost:19091`, `localhost:29091`, `localhost:39091`.
- Always pass all three brokers for resilience.
- Use the `kafka.bootstrap.servers` option (note the `kafka.` prefix required by the Spark Kafka connector).

## Batch Read from Kafka

```python
df = (spark.read.format("kafka")
      .option("kafka.bootstrap.servers", bootstrap_servers)
      .option("subscribe", topic)          # single topic
      # .option("subscribePattern", "top.*")  # topic pattern
      # .option("assign", '{"topic":[0,1]}')  # specific partitions
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load())
```

- The resulting DataFrame has columns: `key`, `value`, `topic`, `partition`, `offset`, `timestamp`, `timestampType`, `headers`.
- **Always cast** binary key/value to string:
  ```python
  df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  ```

## Batch Write to Kafka

```python
df.write.format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("topic", topic) \
    .save()
```

- The DataFrame must have a `value` column (string or binary). The `key` column is optional.

## Structured Streaming — Read

```python
df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", bootstrap_servers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")  # or "latest", or JSON offset spec
      .load())
```

## Structured Streaming — Write

- **foreachBatch pattern** (primary pattern in this project):
  ```python
  query = (df.writeStream
           .foreachBatch(process_batch)
           .start())
  query.awaitTermination()
  ```
  Inside `process_batch(batch_df, batch_id)`:
  1. Guard with `if not batch_df.isEmpty()`.
  2. Persist the batch DataFrame for multiple actions.
  3. Write data (e.g., JSON files via `batch_df.write.format("json").save(...)`).
  4. Compute and save latest offsets to MySQL.
  5. Unpersist the batch DataFrame.

- **Output modes:** `append` (default), `complete`, `update`.
- **Trigger options:**
  ```python
  .trigger(processingTime="10 seconds")
  .trigger(once=True)
  .trigger(availableNow=True)
  .trigger(continuous="1 second")
  ```

## Offset Management

- Track the latest offset per topic-partition using a Window function:
  ```python
  window_spec = Window.partitionBy("topic", "partition").orderBy(col("offset").desc())
  latest = batch_df.withColumn("rnk", rank().over(window_spec)).where("rnk=1")
  ```
- Aggregate offsets into JSON per topic and write to MySQL via JDBC.
- Use `ConfigReader` to load JDBC connection properties from external config files.

## Checkpoint and Fault Tolerance

- Set `checkpointLocation` on the streaming query for exactly-once guarantees:
  ```python
  .option("checkpointLocation", "/path/to/checkpoint")
  ```
- Checkpoint location must be unique per query.

## Watermarking and Windowed Aggregations

- Apply watermarks to handle late data:
  ```python
  df.withWatermark("timestamp", "10 minutes")
  ```
- Use window aggregations for time-based grouping:
  ```python
  from pyspark.sql import functions as F
  df.groupBy(F.window("timestamp", "5 minutes")).agg(...)
  ```

## Kafka Headers

- Access headers from the `headers` column in the Kafka DataFrame.
- Headers are an array of structs with `key` (string) and `value` (binary) fields.

## Stream Lifecycle

- Use `StreamingQueryListener` for monitoring query progress events.
- Implement graceful termination patterns (e.g., signal handling, external stop flags).

## Schema Handling

- Kafka `value` is binary — parse with `from_json()` using an explicit `StructType` schema.
- Use schema inference on a sample batch for prototyping; enforce explicit schemas in production.

## Partition Discovery

- For file-based sources used alongside Kafka, enable partition discovery with appropriate options.
