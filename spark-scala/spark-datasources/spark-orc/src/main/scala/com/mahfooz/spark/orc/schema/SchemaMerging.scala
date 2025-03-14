/*

Like Protocol Buffer, Avro, and Thrift, ORC also supports schema evolution.
Users can start with a simple schema, and gradually add more columns to the schema as needed.
In this way, users may end up with multiple ORC files with different but mutually compatible schemas.
The ORC data source is now able to automatically detect this case and merge schemas of all these files.

Since schema merging is a relatively expensive operation, and is not a necessity in most cases, we turned it off by default.

You may enable it by

1. setting data source option mergeSchema to true when reading ORC files, or
2. setting the global SQL option spark.sql.orc.mergeSchema to true.

 */
package com.mahfooz.spark.orc.schema

import org.apache.spark.sql.{SaveMode, SparkSession}

object SchemaMerging {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SchemaMerging")
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

    val dataHome = sys.env.getOrElse("DATA_HOME","orc") +"/FileData/Orc/Schema"

    //Save as Orc files
    df1.write.format("orc")
      .mode(SaveMode.Overwrite)
      .save(s"${dataHome}/orc-schema-merge")

    df2.write.format("orc")
      .mode(SaveMode.Append)
      .save(s"${dataHome}/orc-schema-merge")

    val df = spark.read.format("orc")
      .option("mergeSchema","true")
      .load(s"${dataHome}/orc-schema-merge")

    df.show()
  }
}
