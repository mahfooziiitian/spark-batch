/*

Spark RDD cache() method by default saves RDD computation to storage level MEMORY_ONLY meaning it will store the
data in the JVM heap as un-serialized objects.
Spark cache() method in RDD class internally calls persist() method which in turn uses
sparkSession.sharedState.cacheManager.cacheQuery to cache the result set of RDD
Cache stores the intermediate results in MEMORY only. i.e. default storage of RDD cache is memory.
RDD cache is merely persist with the default storage level MEMORY_ONLY.
Spark persists shuffle operations (e.g., reduceByKey) with intermediate data
automatically even without calling the persist method.
This avoids re-computation of the entire input if a node fails during the shuffle.
 */
package com.mahfooz.rdd.caching.storage

import org.apache.spark.sql.SparkSession

object CachingRDD {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("CachingRDD")
      .master("local[*]")
      .getOrCreate()

    val fileRDD = spark.sparkContext.wholeTextFiles(
      getClass
        .getResource("/spark_test.txt")
        .getFile
    )
    fileRDD.foreach(println)
    val words = fileRDD.flatMap(line => line._2.split(" "))
    val wordsCached = words.cache()
    println(words.getStorageLevel)
    print(wordsCached.isCheckpointed)
    wordsCached.foreach(record => println(record))
    spark.stop()
  }

}
