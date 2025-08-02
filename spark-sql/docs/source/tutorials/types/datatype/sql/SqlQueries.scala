/*

The programmatic way of issuing SQL queries is to use the sql function of the SparkSession class

 */
package com.mhafooz.spark.sql.datatype.sql

import org.apache.spark.sql.SparkSession

object SqlQueries {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SqlQueries")
      .getOrCreate()

    val infoDF = spark.sql("select current_date() as today , 1 + 100 as value")
    infoDF.show

    val movies = spark.read.parquet(args(0));

    movies.createOrReplaceTempView("movies")

    import spark.implicits._

    spark
      .sql(
        "select * from movies where actor_name like '%Jolie%' and produced_year > 2009"
      )
      .show

    spark
      .sql(
        "select actor_name, count(*) as count from movies group by actor_name"
      )
      .where('count > 30)
      .orderBy('count.desc)
      .show

    spark.stop()
  }
}
