package com.mahfooz.spark.strings.translate

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.translate

object TranslateCharacter {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(TranslateCharacter.getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._

    // transform a string with concatenation, uppercase, lowercase and reverse
    val sparkAwesomeDF =
      Seq(("Spark", "is", "awesome")).toDF("subject", "verb", "adj")

    // translate from one character to another
    sparkAwesomeDF
      .select('subject, translate('subject, "ar", "oc").as("translate"))
      .show

  }

}
