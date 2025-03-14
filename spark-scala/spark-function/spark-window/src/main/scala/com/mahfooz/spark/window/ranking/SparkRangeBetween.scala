package com.mahfooz.spark.window.ranking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SparkRangeBetween {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .appName(SparkRangeBetween.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    import spark.implicits._

    // small shopping transaction data set for two users
    val txDataDF= Seq(("John", "2017-07-02", 13.35),
      ("John", "2017-07-06", 27.33),
      ("John", "2017-07-04", 21.72),
      ("Mary", "2017-07-07", 69.74),
      ("Mary", "2017-07-01", 59.44),
      ("Mary", "2017-07-05", 80.14))
      .toDF("name", "tx_date", "amount")

    // use rangeBetween to define the frame boundary that includes all the rows in each frame
    val forEntireRangeWindow = Window.partitionBy("name")
      .orderBy(desc("amount"))
      .rangeBetween(Window.unboundedPreceding,
        Window.unboundedFollowing)

    // apply the max function over the amount column and then compute the difference
    val amountDifference = max(txDataDF("amount")).over(forEntireRangeWindow) -
      txDataDF("amount")

    // add the amount_diff column using the logic defined above
    val txDiffWithHighestDF = txDataDF.withColumn("amount_diff",
      round(amountDifference, 3))

    // display the result
    txDiffWithHighestDF.show
  }
}