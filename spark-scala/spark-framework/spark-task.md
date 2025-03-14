A task is a unit of work that will be sent to one executor.

Specifically speaking, it is a command sent from the driver program to an 
executor by serializing your Function object.

The executor deserializes the command (as it is part of your JAR that has already been loaded) and executes it on a partition.

A partition is a logical chunk of data distributed across a Spark cluster.

In most cases Spark would be reading data out of a distributed storage, and would partition the data in order to parallelize the processing across the cluster. For example, if you are reading data from HDFS, a partition would be created for each HDFS partition. Partitions are important because Spark will run one task for each partition.

This therefore implies that the number of partitions are important. 

Spark therefore attempts to set the number of partitions automatically unless you specify the number of partitions manually e.g. sc.parallelize (data,numPartitions).