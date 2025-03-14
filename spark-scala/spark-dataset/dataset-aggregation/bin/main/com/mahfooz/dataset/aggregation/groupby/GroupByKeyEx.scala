/*

groupByKey[K: Encoder](func: T => K): KeyValueGroupedDataset[K, T]

 */
package com.mahfooz.dataset.aggregation.groupby

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.scalalang.typed

case class Token(name: String, productId: Int, score: Double)

object GroupByKeyEx {


  def start(): SparkSession ={
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("GroupByKeyEx")
      .getOrCreate()
    spark
  }

  def main(args: Array[String]): Unit = {

    val spark =start()

    import spark.implicits._

    val data = Seq(
      Token("aaa", 100, 0.12),
      Token("aaa", 200, 0.29),
      Token("bbb", 200, 0.53),
      Token("bbb", 300, 0.42)
    )

    val tokens = data.toDS.cache

    val q = tokens.
      groupByKey(_.productId).
      agg(typed.sum[Token](_.score)).
      toDF("productId", "sum").
      orderBy('productId)

    q.show()

  }

}
