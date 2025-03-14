/*

val fileRDD = spark.sparkContext.textFile("/tmp/data.txt")
textFile() can be used to load textFiles into an RDD and each line becomes an element in the RDD.
 */
package com.mahfooz.spark.rdd.datasource.file

import org.apache.spark.sql.SparkSession

object FileDataSources {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val sc=spark.sparkContext

    val fileRDD=sc.textFile("D:\\data\\processing\\spark\\text\\spark_test.txt");

    fileRDD.foreach(println)

    //saving text file
    fileRDD.saveAsTextFile("D:\\data\\processing\\spark\\text\\out.txt")

  }

}
