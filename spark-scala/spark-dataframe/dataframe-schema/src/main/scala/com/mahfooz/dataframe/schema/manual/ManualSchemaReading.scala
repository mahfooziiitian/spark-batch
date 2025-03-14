package com.mahfooz.dataframe.schema.manual

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.types.Metadata

object ManualSchemaReading {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("ManualSchema")
      .master("local")
      .getOrCreate()

    val myManualSchema = StructType(
      Array(
        StructField("DEST_COUNTRY_NAME", StringType, true),
        StructField("ORIGIN_COUNTRY_NAME", StringType, true),
        StructField(
          "count",
          LongType,
          false,
          Metadata.fromJson("{\"hello\":\"world\"}")
        )
      )
    )

    val df = spark.read
      .format("json")
      .schema(myManualSchema)
      .load(getClass.getResource("/2015-summary.json").getFile)

    df.printSchema()
  }
}
