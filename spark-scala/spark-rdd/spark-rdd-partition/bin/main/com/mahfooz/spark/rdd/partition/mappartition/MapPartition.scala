/*
This is exactly the same as map(); the difference being, Spark mapPartitions() provides a
facility to do heavy initializations (for example Database connection) once for each partition
instead of doing it on every DataFrame row.
This helps the performance of the job when you dealing with heavy-weighted initialization on larger
datasets.

mapPartitions() can be used as an alternative to map() and foreach() .
mapPartitions() can be called for each partitions while map() and foreach() is called for each
elements in an RDD
Hence one can do the initialization on per-partition basis rather than each element basis.

 */
package com.mahfooz.spark.rdd.partition.mappartition
import org.apache.spark.sql.SparkSession

object MapPartition {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(MapPartition.getClass.getName)
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7), 2)
    rdd
      .mapPartitions(iterable => {
        println("in partition")
        iterable.map { x =>
          x * 2
        }
      })
      .collect()

    spark.stop()

  }
}
