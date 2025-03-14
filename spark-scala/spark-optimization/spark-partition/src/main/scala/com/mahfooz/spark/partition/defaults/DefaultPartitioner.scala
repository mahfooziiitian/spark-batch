/*

Spark has a default and custom partitioner.
When you create an RDD, you may let the Spark to set the number of partitions (default), or you may set it
explicitly (custom).
The number of partitions in the default case depends on the cluster size and available resources.
Most of the times, the default case will work just fine, but if you are an experienced Spark programmer,
then you may set the number of partitions explicitly.
The default partitioning of an RDD and DataFrame happens when a programmer does not set the number of partition
 explicitly.
In this case, the number of partitions depends on data and resources available in the cluster.
For production environments, most of the time, the default partitioner will work well.
Make sure that no cluster nodes/executors are idle.
When you create an RDD or a DataFrame, there is an option for setting the number of partitions.

 */
package com.mahfooz.spark.partition.defaults

import org.apache.spark.sql.SparkSession

object DefaultPartitioner {

  def main(args: Array[String]): Unit ={
    val spark=SparkSession
      .builder()
      .appName("DefaultPartitioner")
      .master("local[*]")
      .getOrCreate()

    val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
    val rdd = spark.sparkContext.parallelize(numbers)
    rdd.partitions.foreach(records => println(records.index))
  }

}
