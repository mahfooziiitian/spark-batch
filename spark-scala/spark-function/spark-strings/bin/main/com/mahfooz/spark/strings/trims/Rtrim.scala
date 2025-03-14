package com.mahfooz.spark.strings.trims

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ltrim, rtrim, trim}

object Rtrim {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(Rtrim.getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._

    val sparkDF = Seq((" Spark ")).toDF("name")

    // trimming
    sparkDF
      .select(
        trim('name).as("trim"),
        ltrim('name).as("ltrim"),
        rtrim('name).as("rtrim")
      )
      .show

  }

}
