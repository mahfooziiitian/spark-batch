package com.mahfooz.spark.schema.encoder.cases

import com.mahfooz.spark.schema.encoder.model.Record
import org.apache.spark.sql.Encoders

object CaseClassEncoder {
  def main(args: Array[String]): Unit = {
    Encoders.product[Record].schema.printTreeString
  }
}
