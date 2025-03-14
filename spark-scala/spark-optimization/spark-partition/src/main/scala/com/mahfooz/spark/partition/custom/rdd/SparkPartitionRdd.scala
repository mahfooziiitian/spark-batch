/*
For example, when creating an RDD, you may set the number of partitions with using the following API
(numSlices represents the number of custom partitions) to distribute a local Python collection to form an RDD.

 */
package com.mahfooz.spark.partition.custom.rdd

import org.apache.spark.sql.SparkSession

object SparkPartitionRdd {

  def main(args: Array[String]): Unit ={
    val spark=SparkSession
      .builder()
      .appName("SparkPartitionRdd")
      .master("local[*]")
      .getOrCreate()

    val collection=List(1,2,3,4,5,6)

    val rdd=spark.sparkContext.parallelize(collection, numSlices=5)

   rdd.foreach(record => print(record))

    spark.close()

  }

}
