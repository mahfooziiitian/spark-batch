# Spark Job

A **Spark job** is a unit of execution in Apache Spark that reads input data from a file system, performs distributed computations, and writes the results back to the file system.

When a Spark action (such as `save`, `collect`, or `count`) is triggered, Spark creates a job that is divided into multiple parallel tasks. These tasks are distributed across the cluster, enabling efficient large-scale data processing.

**Key characteristics:**

- Reads input data from storage (e.g., HDFS, S3, local files).
- Executes transformations and actions using Spark's distributed engine.
- Writes output data to storage.
- Consists of multiple tasks running in parallel.

**Example Spark actions that spawn jobs:**

- `collect()`
- `saveAsTextFile()`
- `count()`

Spark jobs are fundamental to scalable data analytics and ETL workflows.
