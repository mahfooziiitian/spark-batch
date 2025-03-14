package com.mahfooz.sparksql.schema.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{
  LongType,
  Metadata,
  StringType,
  StructField,
  StructType
}

object SparkSchema {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val myManualSchema = StructType(
      Array(
        StructField("DEST_COUNTRY_NAME", StringType, nullable = true),
        StructField("ORIGIN_COUNTRY_NAME", StringType, nullable = true),
        StructField(
          "count",
          LongType,
          nullable = false,
          Metadata.fromJson("{\"hello\":\"world\"}")
        )
      )
    )

    val flightData2015 = spark.read
      .format("json")
      .schema(myManualSchema)
      .load(args(0))

    flightData2015.printSchema()

    flightData2015.createOrReplaceTempView("flight_data_2015")

    val sqlWay = spark.sql("""SELECT DEST_COUNTRY_NAME, count(1)
         FROM flight_data_2015
         GROUP BY DEST_COUNTRY_NAME
         """)

    sqlWay.printSchema()

    val dataFrameWay = flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .count()

    dataFrameWay.printSchema()
  }
}
