package com.mahfooz.spark.df.reader.csv

import com.mahfooz.spark.df.schema.MovieSchema
import org.apache.spark.sql.SparkSession

object TscFileReader {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()


    val movies = spark.read
      .option("header", "true")
      .option("sep", "\t")
      .schema(MovieSchema.movieSchema)
      .csv("D:\\data\\movies\\movies.tsv")

    movies.printSchema

    movies.show()
  }

}
