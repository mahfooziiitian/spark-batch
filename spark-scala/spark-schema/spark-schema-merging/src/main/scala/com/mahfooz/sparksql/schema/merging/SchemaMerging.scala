package com.mahfooz.sparksql.schema.merging

import org.apache.spark.sql.{SaveMode, SparkSession}

object SchemaMerging {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SchemaMerging")
      .config("spark.master", "local[*]")
      .getOrCreate()

    // This is used to implicitly convert an RDD to a DataFrame.
    import spark.implicits._

    val dataHome = s"${sys.env("DATA_HOME")}/FileData/Parquet/schema/merging_scala"

    // Create a simple DataFrame, store into a partition directory
    val squaresDF = spark.sparkContext
      .makeRDD(1 to 5)
      .map(i => (i, i * i))
      .toDF("value", "square")

    squaresDF.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${dataHome}/test_table/key=1")

    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column
    val cubesDF = spark.sparkContext
      .makeRDD(6 to 10)
      .map(i => (i, i * i * i))
      .toDF("value", "cube")

    cubesDF.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${dataHome}/test_table/key=2")

    // Read the partitioned table
    val mergedDF = spark.read
      .option("mergeSchema", "true")
      .parquet(s"${dataHome}/test_table")

    mergedDF.printSchema()

    mergedDF.show(20)
  }
}
