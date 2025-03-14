package com.mahfooz.spark.rdd.partition.partitioners.hash

object HashPartitionWorking {
  def main(args: Array[String]): Unit = {
    val str = "hello"
    println(str.hashCode)
    val numPartitions = 8
    val partitionIndex = str.hashCode % numPartitions
    println(partitionIndex)
  }
}
