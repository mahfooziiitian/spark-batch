/*
For instance, let’s say a Spark
application has ten executors and each one has 6GB of RAM. If the size of an RDD you
would like to persist in memory is 70GB, then that wouldn’t fit into 60GB of RAM. This
is where the storage-level concept comes in. There are two options that you can specify
when persisting the data of an RDD in memory: location and serialization. The location
option determines whether the data of an RDD should be stored in memory or on disk
or a combination of the two. The serialization option determines whether the data in
the RDD should be stored as a serialized object or not.
Option Memory Space CPU Time In Memory On Disk
MEMORY_ONLY High Low Yes No
MEMORY_AND_DISK High Medium Some Some
MEMORY_ONLY_SER Low High Yes No
MEMORY_AND_DISK_SER Low High Some Some
DISK_ONLY Low High No Yes

If the data of an RDD is no longer needed to be persisted in memory, then you can
use the unpersist() function to remove it from memory to make room for other RDDs.

 */
package com.mahfooz.spark.rdd.types

object RddType {

}
