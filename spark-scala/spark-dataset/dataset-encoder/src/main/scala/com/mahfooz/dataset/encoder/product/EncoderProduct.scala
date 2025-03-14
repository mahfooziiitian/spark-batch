package com.mahfooz.dataset.encoder.product

import org.apache.spark.sql.Encoders

import java.sql.Timestamp

case class WebLog(
    host: String,
    timestamp: Timestamp,
    request: String,
    http_reply: Int,
    bytes: Long
)
object EncoderProduct {

  def main(args: Array[String]): Unit = {
    val webLogSchema = Encoders.product[WebLog].schema
    println(webLogSchema)
  }

}
