package com.mahfooz.spark.df.schema

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object FlightTravelSchema {

  val myManualSchema = new StructType(
    Array(
      new StructField("DEST_COUNTRY_NAME", StringType, true),
      new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      new StructField("count", LongType, false)
    )
  )
}
