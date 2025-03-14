package com.mahfooz.spark.schema.encoder

import org.apache.spark.sql.Encoders

object SparkSchemaEncoder {

  def main(args: Array[String]): Unit = {
    Encoders.product[(Integer, String)].schema.printTreeString
  }
}
