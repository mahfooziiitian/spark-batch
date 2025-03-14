package com.mahfooz.spark.rdd.partition.partitionby

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object PartitionByEx2 {

  def main(args: Array[String]): Unit = {

    val sparkSession=SparkSession
      .builder()
      .appName("partition-information-2")
      .master("local[*]")
      .getOrCreate()
    val numbers=1 to 20

    val rdd = sparkSession.sparkContext.parallelize(numbers)
    .map(num => (num, num))
      //partition = partitionFunc(key) % num_partitions
      .partitionBy(new HashPartitioner(2))
    .persist()

    println("Default parallelism: "+sparkSession.sparkContext.defaultParallelism)
    println("Number of partitions: "+rdd.getNumPartitions)
    println("Partitioner: "+rdd.partitioner)

    //It return an RDD created by coalescing all elements within each partition into a list.
    rdd.glom().collect().foreach(record=>println(record.mkString(",")))

  }

}
