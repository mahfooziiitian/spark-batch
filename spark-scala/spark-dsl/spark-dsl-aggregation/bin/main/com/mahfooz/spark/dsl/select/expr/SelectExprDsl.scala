/*

This transformation is a variant of the select transformation.
The one big difference is that it accepts one or more SQL expressions, rather than columns.

 */
package com.mahfooz.spark.dsl.select.expr

import org.apache.spark.sql.SparkSession

object SelectExprDsl {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SelectExprDsl")
      .getOrCreate()

    val movies =
      spark.read.parquet(args(0))

    movies
      .selectExpr("*", "(produced_year - (produced_year % 10)) as decade")
      .show(5)

    movies
      .selectExpr(
        "count(distinct(movie_title)) as movies",
        "count(distinct(actor_name)) as actors"
      )
      .show

    spark.stop()

  }
}
