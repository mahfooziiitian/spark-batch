package com.mahfooz.sparksql.schema.cases.convert

import org.apache.spark.sql.SparkSession

case class RawPanda(id: Long, zip: String, pt: String,
                    happy: Boolean, attributes: Array[Double])

case class PandaPlace(name: String, pandas: Array[RawPanda])
object CaseToSchema2 {

  private def createAndPrintSchema(session: SparkSession): Unit = {
    val rowPanda = RawPanda(1, "M1B 5K7", "giant", happy = true, Array(0.1, 0.1))
    val pandaPlace = PandaPlace("toronto", Array(rowPanda))
    val df = session.createDataFrame(Seq(pandaPlace))
    df.printSchema()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("case_to_schema").master("local[*]").getOrCreate()
    createAndPrintSchema(spark)
  }

}
