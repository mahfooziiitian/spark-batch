package com.mahfooz.rdd.caching.storage.memory

import org.apache.spark.sql.SparkSession

object CachingRddMemory {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("CachingRddMemory")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val dataPath = s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}/FileData/Text/text-book.txt"
    val words = sc.textFile(dataPath).flatMap(lines => lines.split(" "))

    //caching in memory
    val wordsCached = words.cache()

    //Get the storage level
    println(words.getStorageLevel)

    //Checking the checkpoint
    print(wordsCached.isCheckpointed)
    wordsCached.foreach(record => println(record))
    spark.stop()
  }

}
