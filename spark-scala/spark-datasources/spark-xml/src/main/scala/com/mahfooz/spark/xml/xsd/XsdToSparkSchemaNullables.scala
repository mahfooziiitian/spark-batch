package com.mahfooz.spark.xml.xsd

import com.databricks.spark.xml.util.XSDToSchema
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.nio.file.Paths

object XsdToSparkSchemaNullables {

  private def setNullable(schema: StructType, nullable: Boolean = false): StructType = {
    def recurNullable(schema: StructType): Seq[StructField] =
      schema.fields.map {
        case StructField(name, dtype: StructType, _, meta) =>
          StructField(name, StructType(recurNullable(dtype)), nullable, meta)
        case StructField(name, dtype: ArrayType, _, meta) => dtype.elementType match {
          case struct: StructType => StructField(name, ArrayType(StructType(recurNullable(struct)), true), nullable, meta)
          case other => StructField(name, other, nullable, meta)
        }
        case StructField(name, dtype, _, meta) =>
          StructField(name, dtype, nullable, meta)
      }

    StructType(recurNullable(schema))
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("xsd-to-schema")
      .master("local[*]")
      .getOrCreate()

    val xmlFile = sys.env.getOrElse("DATA_HOME","data")+"\\FileData\\Xml\\item_exports.xml"
    val xsdFile = sys.env.getOrElse("DATA_HOME","data")+"\\FileData\\Xml\\item_exports.xsd"
    val schema = spark
      .createDataFrame(
        spark
          .sparkContext
          .emptyRDD[Row],
        XSDToSchema
          .read(Paths.get(xsdFile))
      )
      .select("ItemExport.Item.*")
      .schema

    val df = spark.read.format("xml")
      .option("rowTag", "Item")
      .schema(setNullable(schema, nullable = true)) // To match XSD & XML file content setting all columns are optional i.e nullable=true
      .load(xmlFile)

    df.printSchema()
    df.show(false)
  }

}
