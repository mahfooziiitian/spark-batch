/*

case class in the Scala language is like a JavaBean class in the Java language.
The Dataframe API provides a Tungsten execution backend that handles the memory management explicitly and generates bytecode dynamically.
The Dataset API provides the encoder that handles the conversion from JVM objects to table format using Spark internal Tungsten binary format.

 */
package com.mahfooz.spark.dataset.encode

import com.mahfooz.spark.dataset.model.Client
import org.apache.spark.sql.SparkSession

object SparkEncoder {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkEncoder")
      .getOrCreate()

    val jsoFilePath="d:/data/spark/json/client.json"

    import spark.implicits._

    val ds=spark.read
      .json(jsoFilePath).as[Client]

    ds.printSchema()

    val dsNew=ds.filter(r=>r.age>18)
      .map(c=>(c.age,c.countryCode))
      .groupBy($"_2")
      .avg()

    dsNew.show()

    spark.stop()

  }
}
