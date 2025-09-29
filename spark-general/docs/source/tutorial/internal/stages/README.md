# Stages

In Spark, jobs are divided into **stages** to optimize execution and fault tolerance.  
Stages represent a set of tasks that can be executed in parallel, based on data partitioning and dependencies.

Stages are classified as **Map stages** or **Reduce stages**:

- **Map stages**: Perform transformations that do not require data shuffling, such as `map`, `filter`, or `flatMap`.
- **Reduce stages**: Involve operations that require shuffling data across partitions, such as `reduceByKey`, `groupByKey`, or `join`.

Each job is split into smaller sets of tasks called stages, which depend on each other. This division is similar to the **map** and **reduce** phases in MapReduce, but Spark's DAG scheduler allows for more flexible and efficient execution.

**Key Points:**

- Stages are determined by wide dependencies (shuffle boundaries).
- Tasks within a stage are executed in parallel across partitions.
- Proper stage division improves performance and fault tolerance.

> Understanding stages is crucial for optimizing Spark jobs and troubleshooting performance issues.
