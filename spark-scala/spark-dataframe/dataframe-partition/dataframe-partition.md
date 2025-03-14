The main idea of partitioning is to optimize job performance.

Job performance can be obtained by ensuring each workload is distributed equally to each Spark executor.

# Why Partition Data?

Partition helps in localizing data and reducing data shuffling across the network nodes, reducing network latency, which is a major component of the transformation operation, thereby reducing the time of completion.

First, we need to ensure that, there is no executor without work, and none of the executors become a bottleneck due to unbalanced work assignments.

# Avoid having big files
Each Spark executor can handle one partition at a time, if we have fewer partitions than our Spark executors, the remaining executors will stay idle and we are not able to take complete advantage of the existing resources.

# Avoid having lots of small files
It requires more network communication to access each small file sitting on the data lake (such as AWS S3, Google Cloud Storage, etc.) and computation may require lots of shuffling of data on disk space.


A good partitioning strategy knows about data and its structure, and cluster configuration. Bad partitioning can lead to bad performance, mostly in 3 fields:

# Too many partitions 
It is regarding your cluster size, and you won’t use efficiently your cluster. For example, it will produce intense task scheduling.

# Not enough partitions 
It is regarding your cluster size, and you will have to deal with memory and CPU issues: memory because your executor nodes will have to put high volume of data in memory (possibly causing OOM Exception), and CPU because compute across the cluster will be unequal.

# Skewed data 
In your partitions can occur.
When a Spark task is executed in these partitioned, they will be distributed across executor slots and CPUs. If your partitions are unbalanced in terms of data volume, some tasks will run longer compared to others and will slow down the global execution time of the tasks (and a node will probably burn more CPU that others).

# Some basic default and desired spark configuration parameters.

1. Default Spark Shuffle Partitions — 200
2. Desired Partition Size (Target Size)= 100 or 200 MB
3. No Of Partitions = Input Stage Data Size / Target Size

# How to decide the partition key(s)?

# Choose low cardinality columns
1. Partition columns (since a HDFS directory will be created for each partition value combination). Generally speaking, the total number of partition combinations should be less than 50K. (For example, don’t use partition keys such as roll_no, employee_ID etc. Instead use the state code, country code, geo_code, etc.)
2. Choose the columns used frequently in filtering conditions.
3. Use at most 2 partition columns as each partition column creates a new layer of directory.

# How to Choose the Number of Partitions

# Lower bound 

2 X number of cores in cluster available to application

# Upper bound

Task should take 100+ ms to execute. 
If it is taking less time, then your partitioned data is too small and your 
application might be spending more time in scheduling the tasks.


# Let’s start by understanding the different methods that exist in PySpark

# Repartitioning

The first way to manage partitions is the repartition operation.
Repartitioning is the operation to reduce or increase the number of partitions in which the data in the cluster will be split. This process involves a full shuffle. Consequently, it is clear that repartitioning is an expensive process. In a typical scenario, most of the data should be serialized, moved, and deserialized.

    repartitioned = df.repartition(8)

In addition to specifying the number of partitions directly, you can pass in the name of the column by which you want to partition the data.

    repartitioned = df.repartition('country')

# Coalesce
The second way to manage partitions is coalesce. This operation reduces the number of partitions and avoids a full shuffle. The executor can safely leave data on a minimum number of partitions, moving data only from redundant nodes. Therefore, it is better to use coalesce than repartition if you need to reduce the number of partitions.
    
    coalesced = df.coalesce(2) 

# PartitionBy
partitionBy(cols) is used to define the folder structure of data.

However, there is no specific control over how many partitions are going to be created.

Different from to coalesce and repartition functions, partitionBy effects the folder structure and does not have a direct effect on the number of partition files that are going to be created nor the partition sizes.
    
    green_df \
        .write \
        .partitionBy("pickup_year", "pickup_month") \
        .mode("overwrite") \
        .csv("data/partitions/partitionBy.csv", header=True)


# RDD operations that hold and propagate a partitioner are-

Join
LeftOuterJoin
RightOuterJoin
GroupByKey
ReduceByKey
FoldByKey
Sort
PartitionBy
FoldByKey


1. Do not partition by columns with high cardinality.
2. Partition by specific columns that are mostly used during filter and groupBy operations.
3. Even though there is no best number, it is recommended to keep each partition file size between 256MB to 1GB.
4. If you are increasing the number of partitions, use repartition()(performing full shuffle).
5. If you are decreasing the number of partitions, use coalesce() (minimizes shuffles).
6. Default no of partitions is equal to the number of CPU cores in the machine.
7. GroupByKey ,ReduceByKey — by default this operation uses Hash Partitioning with default parameters.

# What Is Partitioner?

A partitioner is an object that defines how the elements in a key-value pair RDD are partitioned by key, mapping each key to a partition ID from 0 to numPartitions — 1.

It captures the data distribution at the output. With the help of partitioner, the scheduler can optimize the future operations. The contract of partitioner ensures that records for a given key have to reside on a single partition.
We should choose a partitioner to use for cogroup-like operations. If any of the RDDs already have a partitioner, we should choose that one. Otherwise, we use a default HashPartitioner.

# Spark Partitioning Advantages
Spark is designed to process large datasets 100x faster than traditional processing.

This wouldn't have been possible without partitions.

Below are some advantages of using Spark partitions on memory or on disk.

1. Fast accessed to the data.
2. Provides the ability to perform an operation on a smaller dataset.

Partitioning at rest (disk) is a feature of many databases and data processing frameworks, and it is key to make
reads faster.
