package com.mahfooz.spark.rdd.partition

import org.apache.spark.sql.SparkSession

object PartitionInformation {

  def main(args: Array[String]): Unit = {

    val sparkSession=SparkSession
      .builder()
      .appName("partition-information")
      .master("local[*]")
      .getOrCreate()
    val numbers=1 to 20
    val rdd = sparkSession.sparkContext.parallelize(numbers)

    println("Default parallelism: "+sparkSession.sparkContext.defaultParallelism)
    println("Number of partitions: "+rdd.getNumPartitions)
    println("Partitioner: "+rdd.partitioner)

    //It return an RDD created by coalescing all elements within each partition into a list.
    rdd.glom().collect().foreach(record=>println(record.mkString(",")))

  }

}
