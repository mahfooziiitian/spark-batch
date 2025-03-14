package com.mahfooz.spark.dataframe.row.contact

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col

object ConcateRows {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("ConcateRows")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("json")
      .load(args(0))

    val schema = df.schema

    val newRows = Seq(Row("New Country", "Other Country", 5L),
      Row("New Country 2", "Other Country 3", 1L)
    )

    val parallelizedRows = spark.sparkContext.parallelize(newRows)
    val newDF = spark.createDataFrame(parallelizedRows, schema)

    df.union(newDF)
      .where("count = 1")
      .where(col("ORIGIN_COUNTRY_NAME") =!= "United States")
      .show()
  }

}
