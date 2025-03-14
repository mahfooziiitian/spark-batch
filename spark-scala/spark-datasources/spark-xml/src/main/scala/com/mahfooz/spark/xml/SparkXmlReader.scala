package com.mahfooz.spark.xml


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object SparkXmlReader {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val movieSchema = StructType(
      Array(
        StructField("actor", StringType, true),
        StructField("title", StringType, true),
        StructField("year", LongType, true)
      )
    )

    val dataHome = sys.env.getOrElse("DATA_HOME","data")
    val xmlFile = dataHome+"\\FileData\\Xml\\movies.xml"
    val movies = spark.read
    .format("com.databricks.spark.xml")
    .option("rootTag", "collection")
    .option("rowTag", "movie")
    .load(xmlFile)

    println(movies.rdd.partitions.length)

    movies.count() //job-1
    movies.printSchema
    movies.show(5)

  }
}
