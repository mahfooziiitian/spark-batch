/*

A task is the smallest unit of work that Spark sends to an executor.

It is executed by a thread in an executor on a worker node.

Each task performs some computations to either return a result to a driver program or partition its output for shuffle.

Spark creates a task per data partition. An executor runs one or more tasks concurrently.

The amount of parallelism is determined by the number of partitions.

More partitions mean more tasks processing data in parallel.


 */
package spark.model.executor.task;

public class Tasks {
}
