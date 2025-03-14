package com.mahfooz.spark.strings.padding

import com.mahfooz.spark.strings.trims.Rtrim
import org.apache.spark.sql.functions.{lpad, rpad}
import org.apache.spark.sql.{SparkSession, functions}

object Padding {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(Padding.getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._

    val sparkDF = Seq((" Spark ")).toDF("name")
    sparkDF.select(functions.trim('name).as("trim"))
      .select(lpad('trim, 8, "-").as("lpad"),
        rpad('trim, 8, "=").as("rpad"))
      .show
  }

}
