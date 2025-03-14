package com.mahfooz.spark.caching.memory

import org.apache.spark.sql.SparkSession

object SparkMemoryCaching {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkMemoryCaching")
      .getOrCreate()

    val rdd=spark.sparkContext.parallelize(Seq(1,4,5,6,7,8,3,2,1,8,90))
    print(rdd.partitions.length)
    print(rdd.count)
    rdd.cache()
    print(rdd.count())
    Thread.sleep(200000)
  }

}
