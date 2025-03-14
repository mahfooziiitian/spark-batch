package com.mahfooz.spark.xml.xsd.parsing

import com.databricks.spark.xml.XmlDataFrameReader
import com.databricks.spark.xml.util.XSDToSchema
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.nio.file.Paths

object XmlParsing {

  def setNullable(schema: StructType, nullable: Boolean = false): StructType = {
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
      .appName("XsdParsing")
      .master("local[*]")
      .getOrCreate()

    val xmlFile = sys.env.getOrElse("DATA_HOME", "data") + "\\FileData\\Xml\\xsd_schema\\data\\ESUGWDWH_production_20230509_050157.xml"
    //val xsdFile = sys.env.getOrElse("DATA_HOME", "data") + "\\FileData\\Xml\\xsd_schema\\docprod\\FormModel.xsd"
    //val xsdFile = sys.env.getOrElse("DATA_HOME", "data") + "\\FileData\\Xml\\xsd_schema\\common\\IssuanceModel.xsd"
    val xsdFile = sys.env.getOrElse("DATA_HOME", "data") + "\\FileData\\Xml\\xsd_schema\\dwh\\DWHBatch.xsd"

    val schema = spark
      .createDataFrame(
        spark
          .sparkContext
          .emptyRDD[Row],
        XSDToSchema.read(Paths.get(xsdFile))
      )
      .select("DWHBatch.*")
      .schema

    print(schema.json)

    val df = spark.read
      .option("rowTag", "DWHBatch")
      .option("excludeAttribute","true")
      .option("ignoreNamespace","true")
      .option("inferSchema","false")
      .schema(schema)
      .xml(xmlFile)

    //df.write.mode(SaveMode.Append).saveAsTable("xml_table")

    //df.show()
    //df.select(explode_outer(col("Records.Reinstatement.Entry")).as("records")).withColumn("record_str",to_json(col("records"))).show(truncate = false)
    //df.select(explode_outer(col("Records.Rewrite.Entry")).as("records")).withColumn("record_str",to_json(col("records"))).show(truncate = false)
  }

}
