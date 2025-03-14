package com.mahfooz.spark.strings.regex

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{regexp_extract, regexp_replace}

object SparkRegex {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkRegex")
      .getOrCreate()

    import spark.implicits._

    val rhymeDF = Seq(
      ("A fox saw a crow sitting on a tree singing \"Caw! Caw! Caw!\"")
    ).toDF("rhyme")

    // using a pattern
    rhymeDF
      .select(regexp_extract('rhyme, "[a-z]*o[xw]", 0).as("substring"))
      .show

    // both lines below produce the same output
    rhymeDF
      .select(regexp_replace('rhyme, "fox|crow", "animal").as("new_rhyme"))
      .show(false)

    rhymeDF
      .select(regexp_replace('rhyme, "[a-z]*o[xw]", "animal").as("new_rhyme"))
      .show(false)
  }
}
