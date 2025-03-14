/*

Datasets are the foundational type of the Structured APIs.
Datasets are a strictly Java Virtual Machine (JVM) language feature that work only with Scala and Java.

Dataset<T> in Java and Dataset[T] in Scala.

  val flightsDF =spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("2015-summary.csv")

This is very import as encoder.
  import spark.implicits._

In fact, if you use Scala or Java, all "DataFrames" are actually Datasets of type Row.

Starting with the Spark 2.0 release, there is only one high-level abstraction called a Dataset, which has two flavors:
  a)  a strongly typed API
  b)  an untyped API

DataFrame is essentially a type alias for Dataset[Row], where a Row is a generic untyped JVM object.
Dataset APIs are available in only strongly typed languages such as Scala and Java.
Dataset is known as a data structure in Spark SQL that provides type safety and object-oriented interface.

Dataset:

Acts as an extension to DataFrame API.
Combines the features of DataFrame and RDD.
Serves as a functional programming interface for working with structured data.
Overcomes the limitations of RDD and DataFrames - the absence of automatic optimization in RDD and absence of compile-time type safety in Dataframes.

Features of Dataset
  Offers optimized query using **Tungsten**and Catalyst Query optimizer.
  These will be discussed in upcoming topics.
  Capable of analyzing during compile-time.
  Capable of converting type-safe dataset to untyped DataFrame using methods - toDS():Dataset[A],
  toDF():DataFrame and toDF(columnnames:String):DataFrame.
  Delivers high performance due to faster computation.
  Low memory consumption.
  Provides unified API for Java and Scala.

DataFrames is an organized form of distributed data into named columns that is similar to a table in a relational database.
Dataset is an upgraded release of DataFrame that includes the functionality of object-oriented programming interface, type-safe and fast.

In DataFrames, data is represented as a distributed collection of row objects.
In DataSets, data is represented as rows internally and JVM Objects externally.

After RDD transformation into dataframe, it cannot be regenerated to its previous form.
After RDD transformation into a dataset, it is capable of converting back to original RDD.

In dataframe, a runtime error will occur while accessing a column that is not present in the table. It does not provide compile-time type safety.
In dataset, compile time error will occur in the same scenario as dataset provides the compile-time type safety.

 */

package com.mahfooz.spark.dataset

import com.mahfooz.spark.dataset.model.Flight
import org.apache.spark.sql.SparkSession

object Datasets {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val flightsDF = spark.read
      .parquet(args(0))

    import spark.implicits._

    val flights = flightsDF.as[Flight]

    flights
      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(flight_row => flight_row)
      .limit(5)
      .show()

    flights
      .take(5)
      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(
        fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5)
      )

    flights.show(2)
    println(flights.first.DEST_COUNTRY_NAME)
  }
}
