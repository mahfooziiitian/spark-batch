package com.mahfooz.spark.rdd.partition.glom

import org.apache.spark.sql.SparkSession

object PartitionUsingGlom {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getName)
      .getOrCreate()

    val rdd = spark.range(1,100,1,8).rdd

    println(rdd.getNumPartitions)

    rdd.glom().foreach(record=>{
      record.foreach(element=>print(element+" "))
      println()
    })
  }

}
