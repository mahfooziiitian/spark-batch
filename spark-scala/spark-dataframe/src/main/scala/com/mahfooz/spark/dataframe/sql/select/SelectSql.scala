package com.mahfooz.spark.dataframe.sql.select

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{
  LongType,
  Metadata,
  StringType,
  StructField,
  StructType
}

object SelectSql {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("SelectSql")
      .master("local")
      .getOrCreate()

    val myManualSchema = StructType(
      Array(
        StructField("DEST_COUNTRY_NAME", StringType, true),
        StructField("ORIGIN_COUNTRY_NAME", StringType, true),
        StructField("count", LongType, false)
      )
    )

    val df = spark.read
      .format("json")
      .schema(myManualSchema)
      .load(getClass.getResource("/2015-summary.json").getFile)

    df.select("DEST_COUNTRY_NAME").show(2)

    df.select(
        "SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME FROM dfTable LIMIT 2"
      )
      .show()

  }
}
