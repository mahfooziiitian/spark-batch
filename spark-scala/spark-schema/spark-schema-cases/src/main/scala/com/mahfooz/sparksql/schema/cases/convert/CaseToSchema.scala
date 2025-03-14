package com.mahfooz.sparksql.schema.cases.convert

import com.mahfooz.sparksql.schema.model.{Employee, Name}
import org.apache.spark.sql.Encoders

object CaseToSchema {

  def main(args: Array[String]): Unit = {

    var encoderSchema = Encoders.product[Employee].schema
    encoderSchema.printTreeString()

    encoderSchema = Encoders.product[Name].schema
    encoderSchema.printTreeString()
  }
}
