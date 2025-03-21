package com.mahfooz.spark.confluent

object EchoSchema {
  def main(args: Array[String]): Unit = {
    println("{\"type\":\"record\",\"name\":\"users\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"department\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"modified\",\"type\":{\"type\":\"long\",\"connect.version\":1,\"connect.name\":\"org.apache.kafka.connect.data.Timestamp\",\"logicalType\":\"timestamp-millis\"}}],\"connect.name\":\"users\"}")
  }
}
