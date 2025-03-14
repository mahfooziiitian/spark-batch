package com.mahfooz.dataframe.transformation.projection

import org.apache.spark.sql.SparkSession

object ProjectColumns {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("ProjectColumns")
      .master("local[*]")
      .getOrCreate()

    val df=spark.read.json("D:\\data\\flight-data\\json\\2015-summary.json")

    df.select("DEST_COUNTRY_NAME").show(2)

    df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

  }

}
