/*
wholeTextFiles() can be used to load multiple text files into a paired RDD containing pairs
<filename, textOfFile> representing the filename and the entire content of the file.
This is useful when loading multiple small text files and is different from textFile API because
when whole TextFiles() is used, the entire content of the file is loaded as a single record:

 */
package com.mahfooz.spark.rdd.datasource.file

import org.apache.spark.sql.SparkSession

object ReadWholeTextFile {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ReadWholeTextFile")
      .master("local")
      .getOrCreate()

    val sc=spark.sparkContext

    val fileRDD=sc.wholeTextFiles("D:\\data\\spark\\text")

    fileRDD.foreach(println)
  }
}
