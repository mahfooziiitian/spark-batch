/*

SELECT columnName * 10, otherColumn, someOtherCol as c FROM dataFrameTable

 */
package com.mahfooz.dataframe.transformation.projection

import org.apache.spark.sql.SparkSession

object ProjectColumnExpression {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .appName("ProjectColumnExpression")
      .master("local[*]")
      .getOrCreate()

    val df=spark.read.json("D:\\data\\flight-data\\json\\2015-summary.json")
  }

}
