/*

Optimized Row Columnar (ORC) is another popular open source self-describing columnar storage format in the Hadoop ecosystem.
It was created by a company called Cloudera as part of the initiative to massively speed up Hive.

  spark.read.orc("<path>")
  spark.read.format("orc")

 */
package com.mahfooz.spark.orc.reader

import org.apache.spark.sql.SparkSession

object OrcReader {

  def main(args: Array[String]): Unit = {

    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .config("spark.sql.warehouse.dir",sparkWarehouse)
      .getOrCreate()
    val dataHome = sys.env.getOrElse("DATA_HOME","orc")

    var fileName=s"${dataHome}/FileData/Orc/users.orc"

    if(args.length>0)
      fileName=args(0)

    val movies = spark.read.orc(fileName)
    movies.printSchema
    movies.show(5)

    spark.stop()
  }
}
