/*
A DataFrame is an immutable, distributed collection of data that is organized into rows, where each one
consists a set of columns and each column has a name and an associated type.
In other words, this distributed collection of data has a structure defined by a schema.
Each row in the DataFrame is represented by a generic Row object.

 */
package com.mahfooz.spark.dataframe

import org.apache.spark.sql.SparkSession
import scala.util.Random

object DataFrames {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("DataFrames")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd = sc
      .parallelize(1 to 10)
      .map(x => (x, Random.nextInt(100) * x))

    import spark.implicits._

    val kvDF = rdd.toDF("key", "value")

    kvDF.show

    kvDF.printSchema()
  }
}
