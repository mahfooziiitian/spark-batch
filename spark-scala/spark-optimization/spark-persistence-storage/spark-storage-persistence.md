Caching, through cache() or persist(), which can save your data and the data lineage.
Caching will persist the dataframe in either memory, or disk, or a combination of memory and disk.
Saving the lineage is only useful if you need to rebuild your dataset from scratch, which will happen if one of the nodes of your cluster failed.

# cache

cache() will store as many of the partitions read in memory across Spark executors as memory allows.

DataFrame may be fractionally cached, partitions cannot be fractionally cached (e.g., if you 
have 8 partitions but only 4.5 partitions can fit in memory, only 4 will be cached). 

However, if not all your partitions are cached, when you want to access the data again, the 
partitions that are not cached will have to be recomputed, slowing down your Spark job.
cache() is a synonym of persist(StorageLevel.MEMORY_ONLY)
It will store the RDD composing the dataframe as deserialized Java objects in the JVM.
If the RDD does not fit in memory, Spark will not cache the partitions: Spark will recompute as needed.
You will not be notified.
MEMORY_ONLY is the default level. It will store the RDD composing the dataframe as deserialized Java objects in the JVM. If the RDD does not fit in memory, Spark will not cache the partitions: Spark will recompute as needed. You will not be notified.

·   MEMORY_AND_DISK: similar to MEMORY_ONLY, except that when Spark runs out of memory, it will serialize the RDD on disk. It is slower, as disk is slower, but performance will vary depending on the storage class you may have on your node (NVMe drives vs. mechanical drives…)

·   MEMORY_ONLY_SER: similar to MEMORY_ONLY, however the Java objects are serialized. This should be taking less space but reading will consume more CPU.

·   MEMORY_AND_DISK_SER: similar to MEMORY_AND_DISK with serialization.

·   DISK_ONLY: store the partitions of the RDD composing the dataframe to disk.

·   OFF_HEAP: similar behavior than MEMORY_ONLY_SER, however it uses the off-heap memory. Off-heap usage needs to be activated (see subsection 16.1.3 for more details on memory management).

MEMORY_AND_DISK_2, MEMORY_AND_DISK_SER_2, MEMORY_ONLY_2, and MEMORY_ONLY_SER_2 are equivalent to the ones without the _2, but add replication of each partition on two cluster nodes
]

# persist

persist(StorageLevel.LEVEL) is nuanced, providing control over how your data is cached via StorageLevel

Each StorageLevel (except OFF_HEAP) has an equivalent LEVEL_NAME_2, which means replicate twice on two different Spark executors: MEMORY_ONLY_2, MEMORY_AND_DISK_SER_2.

# When to Cache and Persist

Common use cases for caching are scenarios where you will want to access a large data set repeatedly for queries or transformations. Some examples include:

1. DataFrames commonly used during iterative machine learning training
2. DataFrames accessed commonly for doing frequent transformations during ETL or building data pipelines

# When Not to Cache and Persist

Not all use cases dictate the need to cache.
Some scenarios that may not warrant caching your DataFrames include:

1. DataFrames that are too big to fit in memory 
2. An inexpensive transformation on a DataFrame not requiring frequent use, regardless of size


