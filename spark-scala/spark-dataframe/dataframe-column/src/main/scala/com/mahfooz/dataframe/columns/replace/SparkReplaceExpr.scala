package com.mahfooz.dataframe.columns.replace

import org.apache.spark.sql.SparkSession

object SparkReplaceExpr {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(SparkReplaceExpr.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val movies = spark.read.json("D:\\data\\movies\\movies.json")

    import spark.implicits._

    // now replace the produced_year with new values
    movies.withColumn("produced_year", ('produced_year - 'produced_year % 10)).
      show(5)

  }

}
