package com.everestre.xsd.schema

import com.databricks.spark.xml.util.XSDToSchema

import java.nio.file.Paths

object XsdToSparkSchema {

  def main(args: Array[String]): Unit = {

    val xsdFile = "src/main/resources/schema/dwh/DWHBatch.xsd"
    val schema = XSDToSchema.read(Paths.get(xsdFile))
    print(schema.prettyJson)
  }
}
