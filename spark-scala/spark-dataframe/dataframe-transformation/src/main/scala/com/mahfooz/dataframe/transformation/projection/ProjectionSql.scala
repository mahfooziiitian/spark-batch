/*

SELECT * FROM dataFrameTable

 */
package com.mahfooz.dataframe.transformation.projection

import org.apache.spark.sql.SparkSession

object ProjectionSql {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .appName("ProjectionSql")
      .master("local[*]")
      .getOrCreate()

    val df=spark.read.json("D:\\data\\flight-data\\json\\2015-summary.json")

    df.select("*").show(2)

    df.show(2)

  }

}
