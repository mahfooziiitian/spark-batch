/*
The main purpose of partitioning data is for maximal efficient parallelism (execute many tasks at the same time
using cluster nodes via Spark executors).
Spark Executors are launched at the start of a Spark application in coordination with the Spark Cluster Manager.
Spark Executors are worker nodes’ processes in charge of running individual tasks in a given Spark job/application.
Breaking up data (distributed scheme) into partitions allows Spark executors to process only data that is close
to them, therefore minimizing network bandwidth.
That is, each spark executor’s core is assigned its own data partition to work on.

For example, if an RDD has 10 billion elements, then Spark may partition it into 10,000 chunks (also called
partition number of partitions is 10,000).
In this case, each partition will have about one million elements (10,000 x 1 million = 10 billion).
Then these partitions can be sent to cluster nodes for further transformations such as mappers and reducers.
Partitioning data enable Spark to run many transformations independently and at the same time (by maximizing
parallelism).
Partitions in Spark do not span multiple machines.
This means that each partition is sent to a single worker machine.
Tuples in the same partition are guaranteed to be on the same machine.
Spark assigns one task per partition and each worker can process one task at a time.
Proper partitioning can improve the performance of your data analysis and improper partitioning will hurt the
performance of your data analysis.

For example, imagine a Spark cluster with 501 nodes (one master and 500 worker nodes) and consider an RDD with
10 billion elements.
The proper number partitions would be over 500 (such as 1000) such that all cluster nodes are utilized at the
same time.
What if your number partitions is 100 and each worker can accept at most 2 tasks, then most of your worker nodes
 (about 400 of them) will be idle and useless.
 The more you utilize the worker nodes, the faster your query will run.

 */
package com.mahfooz.spark.partition

class SparkPartition {

}
