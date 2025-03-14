/*
How to Assign a Storage Level?
Let us see how to persist an RDD to a storage level.

 result = input.map(<Computation>)
 result.persist(LEVEL)

By default, Spark uses the algorithm of Least Recently Used (LRU) to remove old and unused RDD to
release more memory.
We can also manually remove remaining RDD from memory by using unpersist().

 */
package com.mahfooz.rdd.caching.storage.level
import org.apache.spark.storage.StorageLevel

object StorageLevelType {
  def main(args: Array[String]): Unit = {

    println(StorageLevel.DISK_ONLY)

  }
}
