/*



 */
package com.mahfooz.dataframe.expression.columns

import org.apache.spark.sql.SparkSession

object SelectAllExpr {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val movies = spark.read.json("D:\\data\\movies\\movies.json");

    movies.selectExpr("*","(produced_year - (produced_year % 10)) as decade").
      show(5)

  }
}
