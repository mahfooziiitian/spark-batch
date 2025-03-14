package com.mahfooz.dataframe.columns.concatenate

import org.apache.spark.sql.functions.{col, concat, concat_ws, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ColumnConcatenate {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ColumnConcatenate")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(("foo", 1), ("bar", 2)).toDF("k", "v")

    df.select(concat($"k", lit(" "), $"v")).show(false)

    //concatenate by handling null
    df.withColumn(
        "concat_col",
        concat(
          when(col("k").isNotNull, col("k")).otherwise(lit("null")),
          when(col("v").isNotNull, col("v")).otherwise(lit("null"))))
      .show(false)


    df.selectExpr("concat(nvl(k, ''), nvl(v, '')) as NEW_COLUMN").show(false)

  }

  def allColumnsConcatenate(dfSource:DataFrame): DataFrame = {
    dfSource.select(concat_ws(",",dfSource.columns.map(c => col(c)): _*))
  }

}
