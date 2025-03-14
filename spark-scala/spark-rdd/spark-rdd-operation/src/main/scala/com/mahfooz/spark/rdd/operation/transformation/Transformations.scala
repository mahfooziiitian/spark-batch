/*

It is a lazy operation and it will produce another RDD.

 */

package com.mahfooz.spark.rdd.operation.transformation

import org.apache.spark.sql.SparkSession


object Transformations {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "E:\\bigdata\\hadoop\\hadoop-2.9.2")

    val spark=SparkSession
      .builder()
      .appName(Transformations.getClass.getName)
      .master("local")
      .getOrCreate();

    val stringList = Array("Spark is awesome","Spark is cool")
    val stringRDD = spark.sparkContext.parallelize(stringList)
    stringRDD.foreach(println)
  }
}
