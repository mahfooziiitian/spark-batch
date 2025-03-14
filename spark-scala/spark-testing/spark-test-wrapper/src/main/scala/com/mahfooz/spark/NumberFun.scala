package com.mahfooz.spark

import org.apache.spark.sql.functions._

object NumberFun {

  def isEven(n: Integer): Boolean = {
    n % 2 == 0
  }

  val isEvenUDF = udf[Boolean, Integer](isEven)

}
