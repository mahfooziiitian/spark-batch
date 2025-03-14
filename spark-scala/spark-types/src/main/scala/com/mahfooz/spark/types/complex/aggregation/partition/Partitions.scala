package com.mahfooz.spark.types.complex.aggregation.partition

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, max, rank, dense_rank}

object Partitions {
  def main(args: Array[String]): Unit = {

    val windowSpec = Window
      .partitionBy("CustomerId", "date")
      .orderBy(col("Quantity").desc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val maxPurchaseQuantity = max(col("Quantity"))
      .over(windowSpec)

    val purchaseDenseRank = dense_rank().over(windowSpec)
    val purchaseRank = rank().over(windowSpec)
    /*
    dfWithDate
      .where("CustomerId IS NOT NULL")
      .orderBy("CustomerId")
      .select(
        col("CustomerId"),
        col("date"),
        col("Quantity"),
        purchaseRank.alias("quantityRank"),
        purchaseDenseRank.alias("quantityDenseRank"),
        maxPurchaseQuantity.alias("maxPurchaseQuantity")
      )
      .show()*/
  }
}
