package com.mahfooz.spark.orc.schema

import org.apache.spark.sql.{SaveMode, SparkSession}

object SchemaReadOnMerge {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SchemaReadOnMerge")
      .config("spark.sql.orc.mergeSchema", "true")
      .getOrCreate()

    import spark.implicits._

    val df1 = Seq(
      ("Category A", 100),
      ("Category B", 200),
      ("Category C", 300)
    ).toDF("col1", "col2")

    val df2 = Seq(
      ("Category D", 100, "D - 1"),
      ("Category E", 200, "E - 1")
    ).toDF("col1", "col2", "col3")

    val dataHome = sys.env
      .getOrElse("DATA_HOME", "orc") + "/FileData/Orc/Schema"

    //Save as Orc files
    df1.write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .save(s"${dataHome}/orc-schema-merge")

    df2.write
      .format("orc")
      .mode(SaveMode.Append)
      .save(s"${dataHome}/orc-schema-merge")

    val df = spark.read
      .format("orc")
      .load(s"${dataHome}/orc-schema-merge")

    df.show()
  }

}
