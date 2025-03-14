/*

Spark Schema defines the structure of the data.

{"name":"mission","pandas":[{"id":1,"zip":"94110","pt":"giant", "happy":true,
        "attributes":[0.4,0.5]}]}

Spark SQL provides StructType & StructField classes to programmatically specify the schema.
By default, Spark infers the schema from data, however, some times we may need to define our own column names
and data types especially while working with unstructured and semi-structured data.
Spark schema is the structure of the DataFrame or Dataset.

 */
package com.mahfooz.sparksql.schema

import org.apache.spark.sql.SparkSession

case class RawPanda(id: Long, zip: String, pt: String,
                    happy: Boolean, attributes: Array[Double])

case class PandaPlace(name: String, pandas: Array[RawPanda])

object SchemaDetails {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .appName(getClass().getSimpleName)
      .master("local")
      .getOrCreate()
    createAndPrintSchema(session)
  }

  def createAndPrintSchema(session: SparkSession) = {
    val damao = RawPanda(1, "M1B 5K7", "giant", true, Array(0.1, 0.1))
    val pandaPlace = PandaPlace("toronto", Array(damao))
    val df = session.createDataFrame(Seq(pandaPlace))
    df.printSchema()
    df.foreach(row => println(row))
  }

}
