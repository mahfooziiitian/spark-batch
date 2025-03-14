package com.mahfooz.spark.rdd.operation.transformation.wide.sort

import org.apache.spark.sql.SparkSession

object SortRdd {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("SortRdd")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val dataPath = s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}/FileData/Text/text-book.txt"

    val data = sc.textFile(dataPath)

    val words = data.flatMap(lines => lines.split(" "))

    words.sortBy(word => word.length() * -1).take(2).foreach(println)
  }

}
