
# Why RDD is slow?

Using RDD directly leads to performance issues as Spark does not know how to apply the optimization techniques and RDD serialize and de-serialize the data when it distributes across a cluster (repartition & shuffling).

Serialization and de-serialization are very expensive operations for Spark applications or any distributed systems, most of our time is spent only on serialization of data rather than executing the operations hence try to avoid using RDD.

1. Space efficiency


# Is DataFrame Faster

Since Spark DataFrame maintains the structure of the data and column types (like an RDMS table) it can handle the data better by storing and managing more efficiently.

The DataFrame API does two things that help to do this (through the Tungsten project). First, using off-heap storage for data in binary format. Second, generating encoder code on the fly to work with this binary format for your specific objects.

Since Spark/PySpark DataFrame internally stores data in binary there is no need of Serialization and deserialization data when it distributes across a cluster hence you would see a performance improvement.




