package com.mahfooz.dataframe.schema.seqdf

import org.apache.spark.sql.SparkSession

object SequenceToDf {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("SequenceToDf")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val movies = Seq(
      ("Damon, Matt", "The Bourne Ultimatum", 2007L),
      ("Damon, Matt", "Good Will Hunting", 1997L)
    )

    val moviesDF = movies.toDF("actor", "title", "year")

    moviesDF.printSchema()

    moviesDF.show()
  }

}
