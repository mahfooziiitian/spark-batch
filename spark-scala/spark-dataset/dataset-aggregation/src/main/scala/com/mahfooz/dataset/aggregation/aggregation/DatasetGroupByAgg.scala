package com.mahfooz.dataset.aggregation.aggregation

import org.apache.spark.sql.SparkSession

case class Token(name: String, productId: Int, score: Double)

object DatasetGroupByAgg {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DatasetGroupByAgg")
      .getOrCreate()

    val data = Seq(
      Token("aaa", 100, 0.12),
      Token("aaa", 200, 0.29),
      Token("bbb", 200, 0.53),
      Token("bbb", 300, 0.42))

    import spark.implicits._

    val tokens = data.toDS.cache

    tokens.groupBy('name).avg().show
    tokens.groupBy('name, 'productId).agg(Map("score" -> "avg")).show
  }

}
