/*
The key abstraction of Spark is RDD.

Resilient Distributed Datasets (RDD) is a fundamental data structure of Spark.

It is an immutable distributed collection of objects.

It is the fundamental unit of data in Spark.

Each dataset in RDD is divided into logical partitions, which may be computed on different nodes of the cluster.

Spark revolves around the concept of a resilient distributed dataset (RDD), which is a fault-tolerant collection of elements that can be operated on in parallel. There are two ways to create RDDs: parallelizing an existing collection in your driver program, or referencing a dataset in an external storage system, such as a shared filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.

Resilient distributed datasets (RDDs) are the known as the main abstraction in Spark.

It is a partitioned collection of objects spread across a cluster, and can be persisted in memory or on disk.

Once created, RDDs are immutable.

 */
package spark.rdd;

public class RDD {

}
