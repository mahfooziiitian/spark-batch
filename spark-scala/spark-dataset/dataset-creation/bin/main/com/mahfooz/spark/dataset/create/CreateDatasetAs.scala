/*

First thing you need to do is to define a domain-specific object to represent each row.
The first way is to transform a DataFrame to a Dataset using the as(Symbol) function of the DataFrame class.

 */
package com.mahfooz.spark.dataset.create

import com.mahfooz.spark.dataset.dataframe.CreateDatasetAs
import com.mahfooz.spark.dataset.model.Movie
import org.apache.spark.sql.SparkSession

object CreateDatasetAs {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(CreateDatasetAs.getClass.getSimpleName)
      .getOrCreate()

    val movies = spark.read.json("D:\\data\\movies\\movies.json")

    import spark.implicits._

    // convert DataFrame to strongly typed Dataset
    val moviesDS = movies.as[Movie]

    moviesDS.printSchema()
  }
}