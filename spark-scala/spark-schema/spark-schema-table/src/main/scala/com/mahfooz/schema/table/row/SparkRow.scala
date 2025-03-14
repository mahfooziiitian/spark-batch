/*

A row is nothing more than a record of data.
Each record in a DataFrame must be of type Row, as we can see when we collect the following DataFrames.
We can create these rows manually from SQL, from Resilient Distributed Datasets (RDDs), from data sources,
or manually from scratch.
 */
package com.mahfooz.schema.table.row

import org.apache.spark.sql.SparkSession

object SparkRow {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkRow")
      .getOrCreate()

    val df=spark.range(2).toDF()
    val dataArray=df.collect()

  }

}
