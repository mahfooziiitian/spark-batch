# How data gets represented
RDD is a JVM object.


1. A set of partitions, which are the chunks that make up the entire dataset 
2. A set of dependencies on parent RDDs 
3. A function for computing all the rows in the data set 
4. Metadata about the partitioning scheme (optional)
5. Where the data lives on the cluster (optional); if the data lives on HDFS, then it would be where the block locations are located


The Spark runtime uses these five pieces of information to schedule and execute the user data processing logic that is expressed via the RDD operations, which are described in the following section.

The first three pieces of information make up the lineage information, which Spark uses for two purposes. The first one is determining the order of execution of RDDs, and the second one is for failure recovery purposes.


Each row in a dataset is represented as a Java object, and the structure of this Java object is opaque to Spark.

The user of RDD has complete control over how to manipulate this Java object



