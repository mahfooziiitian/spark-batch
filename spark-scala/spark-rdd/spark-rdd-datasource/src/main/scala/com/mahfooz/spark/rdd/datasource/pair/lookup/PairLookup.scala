package com.mahfooz.spark.rdd.datasource.pair.lookup

import org.apache.spark.sql.SparkSession

object PairLookup {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val rdd=spark.sparkContext.wholeTextFiles(getClass
      .getResource("/spark_test.txt").getFile)

    val words=rdd.flatMap(line => line._2.split(" "))

    val keyword = words.keyBy(word => word.toLowerCase.toSeq(0).toString)

    keyword.lookup("s")
  }
}
