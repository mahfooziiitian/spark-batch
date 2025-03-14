package com.mahfooz.spark.checkpoint.lineage

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object CacheLineage {

  def main(args: Array[String]): Unit = {

    val spark= SparkSession
      .builder()
      .appName("cache-lineage")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd = sc.parallelize(1 to 10)
      .map(x => (x % 3, 1))
      .reduceByKey(_ + _)

    val indCache  = rdd.mapValues(_ > 4)
    indCache.persist(StorageLevel.DISK_ONLY)

    println(indCache.toDebugString)

    println(indCache.count)

    println(indCache.toDebugString)

  }
}
