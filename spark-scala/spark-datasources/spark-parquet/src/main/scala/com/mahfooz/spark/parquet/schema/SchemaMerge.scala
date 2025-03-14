package com.mahfooz.spark.parquet.schema

import org.apache.spark.sql.{SaveMode, SparkSession}

object SchemaMerge {

  def main(args: Array[String]): Unit = {

    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .config("spark.sql.warehouse.dir",sparkWarehouse)
      .getOrCreate()

    println(spark.conf.get("spark.sql.parquet.mergeSchema"))

    val dataHome = sys.env.getOrElse("DATA_HOME","data")
    val dataPath = s"${dataHome}/FileData/Parquet/Schema/Merge"

    import spark.implicits._

    // Create a simple DataFrame, store into a partition directory
    val squaresDF = spark.sparkContext
      .makeRDD(1 to 5)
      .map(i => (i, i * i))
      .toDF("value", "square")

    squaresDF.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${dataPath}/test_table/key=1")

    squaresDF.show()

    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column
    val cubesDF = spark.sparkContext
      .makeRDD(6 to 10)
      .map(i => (i, i * i * i))
      .toDF("value", "cube")

    cubesDF.show()

    cubesDF.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${dataPath}/test_table/key=2")

    // Read the partitioned table
    val mergedDF = spark.read
      .option("mergeSchema", "true")
      .parquet(s"${dataPath}/test_table")

    mergedDF.printSchema()
    mergedDF.show()
  }
}
