package com.mahfooz.dataset.encoder.product

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.{
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.scalatest.funsuite.AnyFunSuite

class EncoderProductTest extends AnyFunSuite {
  test("encoder product test") {
    val webLogSchema = Encoders.product[WebLog].schema
    val givenSchema = StructType(
      Seq(
        StructField("host", StringType, nullable = true),
        StructField("timestamp", TimestampType, nullable = true),
        StructField("request", StringType, nullable = true),
        StructField("http_reply", IntegerType, nullable = false),
        StructField("bytes", LongType, nullable = false)
      )
    )
    assert(givenSchema === webLogSchema)
  }
}
