package com.mahfooz.dataframe.schema.row


import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.WrappedArray

object DataFrameRowArrayMap {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("DataFrameRowArrayMap")
      .getOrCreate()

    val arrayMapSchema = new StructType()
      .add("name",StringType)
      .add("booksInterested",ArrayType(new StructType()
      .add("name",StringType)
      .add("author",StringType)
      .add("pages",IntegerType)))

    val arrayMapData = Seq(
      Row("James", List(Row("Java","XX",120),Row("Scala","XA",30),
      Row("Michael", List(Map("hair"-> "brown"),Map("eye"-> "black"),Map("height"-> "6"))),
      Row("Robert", List(Map("hair"-> "red"),Map("eye"-> "gray"),Map("height"-> "6.3")))
    )))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayMapData),arrayMapSchema)

    df.printSchema()

    val rows = df.collect()

    for(row <- rows) {
      val properties = row.getAs[WrappedArray[Map[String,String]]]("properties")
      for(property <- properties)
        println(property)
    }

  }

}
