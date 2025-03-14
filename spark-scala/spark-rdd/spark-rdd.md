# RDD
An RDD represents an immutable, partitioned collection of records that can be operated on in parallel.

Virtually all Spark code you run, whether DataFrames or Datasets, compiles down to an RDD.

RDDs the records are just Java, Scala, or Python objects of the programmer’s choosing.

Internally, each RDD is characterized by five main properties:

1. A list of partitions
2. A function for computing each split
3. A list of dependencies on other RDDs
4. Optionally, a Partitioner for key-value RDDs (e.g., to say that the RDD is hashpartitioned)
5. Optionally, a list of preferred locations on which to compute each split (e.g., block
locations for a Hadoop Distributed File System [HDFS] file)

# When to Use the Low-Level APIs?
You should generally use the lower-level APIs in three situations:
1. You need some functionality that you cannot find in the higher-level APIs; for example,
if you need very tight control over physical data placement across the cluster.
2. You need to maintain some legacy codebase written using RDDs.
3. You need to do some custom shared variable manipulation

# When to Use RDDs?
1. In general, you should not manually create RDDs unless you have a very, very specific reason
for doing so. They are a much lower-level API that provides a lot of power but also lacks a lot of
the optimizations that are available in the Structured APIs. For the vast majority of use cases,
2. DataFrames will be more efficient, more stable, and more expressive than RDDs.
3. The most likely reason for why you’ll want to use RDDs is because you need fine-grained
control over the physical distribution of data (custom partitioning of data).

