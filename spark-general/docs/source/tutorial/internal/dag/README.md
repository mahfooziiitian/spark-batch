# Spark DAG

A **Directed Acyclic Graph (DAG)** in Apache Spark represents the sequence of computations performed on data. Each node in the DAG is an RDD (Resilient Distributed Dataset) and edges represent the operations (transformations) applied.

## How Spark DAG Works

1. **Transformations** (e.g., `map`, `filter`) build the DAG by defining a lineage of RDDs.
2. **Actions** (e.g., `collect`, `count`) trigger the execution of the DAG.
3. Spark optimizes the DAG into stages and tasks for efficient execution.

## Example

```scala
val data = sc.textFile("data.txt")
val words = data.flatMap(_.split(" "))
val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
wordCounts.saveAsTextFile("output")
```

This code builds a DAG of RDD transformations and actions.

## Visualization

Spark provides a DAG visualization in the Spark UI under the "Stages" tab, helping users understand job execution and optimize performance.

## References

- [Spark DAG Documentation](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-operations)
- [Understanding Spark DAG](https://databricks.com/glossary/dag)

