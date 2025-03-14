/*

DataFrameReader is a fluent API to describe the input data source that will be used to "load" data from an external
data source (e.g. files, tables, JDBC or Dataset[String])

public class DataFrameReader
  extends Object
    implements Logging

Interface used to load a Dataset from external storage systems (e.g. file systems, key-value stores, etc).

An instance of the DataFrameReader class is available as the read variable of the SparkSession class.

  spark.read.format(...).option("key", value").schema(...).load()

DataFrameReader is created (available) exclusively using SparkSession.read.

  import org.apache.spark.sql.DataFrameReader
  val reader = spark.read
  assert(reader.isInstanceOf[DataFrameReader])

csv
  csv(csvDataset: Dataset[String]): DataFrame
  csv(path: String): DataFrame
  csv(paths: String*): DataFrame

format
  format(source: String): DataFrameReader

jdbc

  jdbc(
    url: String,
    table: String,
    predicates: Array[String],
    connectionProperties: Properties): DataFrame

  jdbc(
    url: String,
    table: String,
    properties: Properties): DataFrame

  jdbc(
    url: String,
    table: String,
    columnName: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int,
    connectionProperties: Properties): DataFrame

json

  json(jsonDataset: Dataset[String]): DataFrame
  json(path: String): DataFrame
  json(paths: String*):

load
  load(): DataFrame
  load(path: String): DataFrame
  load(paths: String*): DataFrame

option
  option(key: String, value: Boolean): DataFrameReader
  option(key: String, value: Double): DataFrameReader
  option(key: String, value: Long): DataFrameReader
  option(key: String, value: String): DataFrameReader

options
  options(options: scala.collection.Map[String, String]): DataFrameReader
  options(options: java.util.Map[String, String]): DataFrameReader

orc
  orc(path: String): DataFrame
  orc(paths: String*): DataFrame

parquet
  parquet(path: String): DataFrame
  parquet(paths: String*): DataFrame

schema
  schema(schemaString: String): DataFrameReader
  schema(schema: StructType): DataFrameReader
table
  table(tableName: String): DataFrame

text
  text(path: String): DataFrame
  text(paths: String*): DataFrame

textFile
  textFile(path: String): Dataset[String]
  textFile(paths: String*): Dataset[String]


DataFrameReader supports many file formats natively and offers the interface to define custom formats.

DataFrameReader assumes parquet data source file format by default that you can change using
  spark.sql.sources.default

 */
package com.mahfooz.spark.ds.dfreader

object DataFrameReaders {
  def main(args: Array[String]): Unit = {}
}
