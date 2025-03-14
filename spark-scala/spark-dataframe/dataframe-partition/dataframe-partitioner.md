# Hash Partitioner
Splits our data in such way that elements with the same hash (can be key, keys, or a function) will be in the same partition. 

We can also pass wanted number of partitions, so that the final determined partition will be hash % numPartitions. 

Notice that if numPartitions is bigger than the number of groups with the same hash, there would be empty partitions.

    df.repartiton(10, 'class_id')

**Hash partitioning can make distributed data skewed**

# Range Partitioner
Very similar to hash partitioning, only it’s based on a range of values.

Due to performance reasons, this method uses a sampling to estimate the ranges.

Hence, the output may be inconsistent, since the sampling can return different values.
The sample size can be controlled by the config value 
    
    spark.sql.execution.rangeExchange.sampleSizePerPartition
    df.repartitionByRange(10, 'grade')


# Round Robin Partitioning
Distributing the data from the source number of partitions to the target number of partitions in a 
round robin way, to keep equal distribution between the resulted partitions.

Since repartitioning is a shuffle operation, if we don’t pass any value, it will use the configuration values mentioned above to set the final number of partitions.
    
    df.repartition(10)

# Limitation in Spark Bucketing

Spark Bucketing has its own limitations and we need to be very cautious when we create the bucketed tables and while we join them together.

To optimize the join and make use of bucketing in Spark we need to be sure of the below:

Both the tables are Bucketed with same number of buckets. If the bucket numbers are different in the joining tables, the pre shuffle will not be applied.
Both the tables are bucketed on the same column for joining. As the data is partitioned based on the given bucketed column, if we do not use the same column for joining, you are not making use of bucketing and it will hit the performance.

# How is Spark Bucketing Different from Hive Bucketing?

In Hive, we need the reducer based on which the number of files need to be created.

Where as, in Spark bucketing, we do not have a reducer. So, it ends up creating n number of files based on the number of tasks.

