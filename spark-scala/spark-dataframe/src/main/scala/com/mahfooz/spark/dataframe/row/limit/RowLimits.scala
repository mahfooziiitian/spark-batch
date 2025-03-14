package com.mahfooz.spark.dataframe.row.limit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object RowLimits {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("RowLimits")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("json")
      .load(getClass.getResource("/2015-summary.json").getFile)

    df.limit(5).show()

    df.orderBy(expr("count desc")).limit(6).show()

  }
}
