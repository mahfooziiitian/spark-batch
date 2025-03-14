/*

DataFrameWriter is the interface to describe how data (as the result of executing a structured query) should be
saved to an external data source.

We access the DataFrameWriter on a per-DataFrame basis via the write attribute.

  movies.write.format(...).mode(...).option(...).partitionBy(...).bucketBy(...).sortBy(...).save(path)

Method	Description

bucketBy
  bucketBy(numBuckets: Int, colName: String, colNames: String*): DataFrameWriter[T]

csv
  csv(path: String): Unit

format
  format(source: String): DataFrameWriter[T]

insertInto
  insertInto(tableName: String): Unit
  Inserts (the results of) a structured query (DataFrame) into a table

jdbc
  jdbc(url: String, table: String, connectionProperties: Properties): Unit

json
  json(path: String): Unit

mode
  mode(saveMode: SaveMode): DataFrameWriter[T]
  mode(saveMode: String): DataFrameWriter[T]

option
  option(key: String, value: String): DataFrameWriter[T]
  option(key: String, value: Boolean): DataFrameWriter[T]
  option(key: String, value: Long): DataFrameWriter[T]
  option(key: String, value: Double): DataFrameWriter[T]

options
  options(options: scala.collection.Map[String, String]): DataFrameWriter[T]

orc
  orc(path: String): Unit

parquet
  parquet(path: String): Unit

partitionBy
  partitionBy(colNames: String*): DataFrameWriter[T]

save
  save(): Unit

save(path: String): Unit
  Saves a DataFrame (i.e. writes the result of executing a structured query) to the data source

saveAsTable
  saveAsTable(tableName: String): Unit

sortBy
  sortBy(colName: String, colNames: String*): DataFrameWriter[T]

text
  text(path: String): Unit

 */
package com.mahfooz.spark.ds.dfwriter

import org.apache.spark.sql.{SaveMode, SparkSession}

object DataFrameWriters {
  def main(args: Array[String]): Unit = {

    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse")

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.warehouse.dir",sparkWarehouse)
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val ids = spark.range(5)

    val dataHome  = sys.env.getOrElse("DATA_HOME","data")

    ids.write
      .mode(SaveMode.Overwrite)
      .save(s"${dataHome}/Processing/Spark/Datasource/five_ids")

    spark.read
      .load(s"${dataHome}/Processing/Spark/Datasource/five_ids")
      .show()
  }
}
