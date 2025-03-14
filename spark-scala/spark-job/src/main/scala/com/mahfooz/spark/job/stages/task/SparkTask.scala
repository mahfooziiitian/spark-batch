/*
Stages in Spark consist of tasks.
Each stage comprises of Spark tasks (a unit of execution) which are then federated across each Spark worker's
Executor.
Each task maps to a single core and works a single partition of data.
Each task corresponds to a combination of blocks of data and a set of transformations that will run on a single executor.
If there is one big partition in our dataset, we will have one task.
If there are 1,000 little partitions, we will have 1,000 tasks that can be executed in parallel.
A task is just a unit of computation applied to a unit of data (the partition).
Partitioning your data into a greater number of partitions means that more can be executed in parallel.
This is not a panacea, but it is a simple place to begin with optimization.

 */
package com.mahfooz.spark.job.stages.task

class SparkTask {

}
