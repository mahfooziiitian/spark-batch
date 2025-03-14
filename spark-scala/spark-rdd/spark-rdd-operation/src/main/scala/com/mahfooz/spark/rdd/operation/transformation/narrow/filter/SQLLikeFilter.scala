package com.mahfooz.spark.rdd.operation.transformation.narrow.filter

import org.apache.spark.sql.SparkSession

object SQLLikeFilter {

  def startsWithS(individual:String) = {
    individual.startsWith("S")
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName ("SQLLikeFilter")
      .master ("local[*]")
      .getOrCreate ()

    val sc=spark.sparkContext

    val dataPath = s"file:///${sys.env.getOrElse("DATA_HOME","data").replace("\\","/")}/FileData/Text/text-book.txt"

    val data = sc.textFile(dataPath)

    val words=data.flatMap(lines => lines.split(" "))

    words.filter(word => startsWithS(word))
      .collect()
      .foreach(println)
  }
}
