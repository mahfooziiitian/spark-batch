/*
During the process of transforming a DataFrame to a Dataset using a Scala case class,
Spark will perform a validation to ensure the member variable names in the Scala case
class matches up with the column names in the schema of the DataFrame. If there is a
mismatch, Spark will let you know.

 */
package com.mahfooz.spark.dataset.dataframe

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
