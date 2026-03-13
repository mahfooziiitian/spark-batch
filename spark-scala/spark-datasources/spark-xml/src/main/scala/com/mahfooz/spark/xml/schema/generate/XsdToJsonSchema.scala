package com.mahfooz.spark.xml.schema.generate
import com.databricks.spark.xml.util.XSDToSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

import java.io.File

class SparkSchemaToJsonFile {
  def sparkSchemaFromXsd(spark: SparkSession, xsdPath: String, rowTagExpression: String): StructType = {
    val schema = spark
      .createDataFrame(
        spark
          .sparkContext
          .emptyRDD[Row],
        XSDToSchema.read(new File(xsdPath))
      )
      .select(rowTagExpression)
      .schema
    schema
  }
}

object XsdToJsonSchema {
  def main(args: Array[String]): Unit = {
    //val xsdFile = sys.env.getOrElse("DATA_HOME",".") +"/file_data/xml/orders.xsd"
    val xsdFile = "src/main/resources/orders.xsd"
    val rowTagExpression = "Orders.Order"
    val spark = SparkSession.builder().master("local[*]").appName("xsd-to-json-schema").getOrCreate()

  val sparkSchemaToJsonFile = new SparkSchemaToJsonFile()
    val schema = sparkSchemaToJsonFile.sparkSchemaFromXsd(spark, xsdFile, rowTagExpression)
    val schemaJson = schema.prettyJson
    println(schemaJson)

  }
}