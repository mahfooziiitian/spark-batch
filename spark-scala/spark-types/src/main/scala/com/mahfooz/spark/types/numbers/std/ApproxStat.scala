package com.mahfooz.spark.types.numbers.std
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

object ApproxStat {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("ApproxStat")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    val colName = "UnitPrice"
    val quantileProbs = Array(0.5)
    val relError = 0.05
    df.stat.approxQuantile("UnitPrice", quantileProbs, relError) // 2.51

    df.stat.crosstab("StockCode", "Quantity").show()
    df.stat.freqItems(Seq("StockCode", "Quantity")).show()

    df.select(monotonically_increasing_id()).show(2)

  }
}
