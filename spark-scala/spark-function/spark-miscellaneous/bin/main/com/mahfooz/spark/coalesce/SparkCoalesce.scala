/*

Spark provides a function called coalesce that takes one or more column values and returns the first one that is not null.
Each argument in the coalesce function must be of type Column, so if you want to fill in a literal value, then you can leverage the lit
function.

 */
package com.mahfooz.spark.coalesce

import com.mahfooz.spark.model.Movie
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkCoalesce {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkRange")
      .getOrCreate()

    import spark.implicits._
    val badMoviesDF = Seq(
      Movie(null, null, 2018L),
      Movie("John Doe", "Awesome Movie", 2018L)
    ).toDF

    // use coalese to handle null value in title column
    badMoviesDF
      .select(coalesce('actor_name, lit("no_name")).as("new_title"))
      .show

  }
}
