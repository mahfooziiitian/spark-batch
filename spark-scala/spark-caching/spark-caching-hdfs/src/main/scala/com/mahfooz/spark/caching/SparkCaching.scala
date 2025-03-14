package com.mahfooz.spark.caching
import org.apache.spark.sql.SparkSession

object SparkCaching {

  def main(args: Array[String]): Unit = {
    val dataPath = s"${sys.env("DATA_HOME").replace("\\","/")}/FileData/Text/input.txt"
    val spark = SparkSession.builder.master("local[*]").appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(dataPath).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
