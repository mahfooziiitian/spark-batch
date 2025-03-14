package com.mahfooz.spark.dataset.typed

import com.mahfooz.dataset.util.PersonData
import org.apache.spark.sql.SparkSession

object ScalaDataSetEx {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("SparkDataSetEx")
      .getOrCreate()

    // load initial RDD
    import spark.implicits._
    val dataset = spark.createDataset(PersonData.read())

    // verbose syntax
    println("Under 21")
    dataset.filter(p => p.age < 21)
      .collect
      .foreach(println)

    // concise syntax
    println("Over 21")
    dataset.filter(_.age > 21)
      .collect
      .foreach(println)

    // terminate spark context
    spark.stop()

  }
}
