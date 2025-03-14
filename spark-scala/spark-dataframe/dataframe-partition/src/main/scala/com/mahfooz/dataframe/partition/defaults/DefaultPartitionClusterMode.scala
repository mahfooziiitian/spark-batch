/*
When you running Spark jobs on the Hadoop cluster the default number of partitions is based on the following.

1) On the HDFS cluster, by default, Spark creates one Partition for each block of the file.
2) In Version 1 Hadoop the HDFS block size is 64 MB and in Version 2 Hadoop the HDFS block size is 128 MB
3) Total number of cores on all executor nodes in a cluster or 2, whichever is larger
For example if you have 640 MB file and running it on Hadoop version 2, creates 5 partitions with each consists
on 128 MB blocks (5 blocks * 128 MB = 640 MB).
If you repartition to 10 then it creates 2 partitions for each block.

 */
package com.mahfooz.dataframe.partition.defaults

class DefaultPartitionClusterMode {

}
