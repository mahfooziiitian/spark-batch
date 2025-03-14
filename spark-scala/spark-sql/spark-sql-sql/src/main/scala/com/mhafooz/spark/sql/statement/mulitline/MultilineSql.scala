package com.mhafooz.spark.sql.statement.mulitline

import org.apache.spark.sql.SparkSession

object MultilineSql {

  def main(args: Array[String]): Unit = {

    val sourceFileName="D:\\data\\movies\\movies.parquet"

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("MultilineSql")
      .getOrCreate()

    val movies = spark.read.parquet(sourceFileName);

    movies.createOrReplaceTempView("movies")

    movies.printSchema()

    import spark.implicits._

    // leverage """ to format multi-line SQL statement
    spark
      .sql(
        """select count(*) as count, produced_year 
          | from movies 
          | group by produced_year""".stripMargin
      )
      .orderBy('count.desc)
      .show(5)

    spark.stop()

  }
}
