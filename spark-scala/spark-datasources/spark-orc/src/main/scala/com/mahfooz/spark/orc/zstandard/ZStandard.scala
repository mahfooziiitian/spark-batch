/*

Spark supports both Hadoop 2 and 3. Since Spark 3.2, you can take advantage of Zstandard compression in ORC files on both Hadoop versions.
Zstandard is a fast compression algorithm, providing high compression ratios.
It also offers a special mode for small data, called dictionary compression.
The reference library offers a very wide range of speed / compression trade-off, and is backed by an extremely fast decoder.
Zstandard library is provided as open source software using a BSD license.

 */
package com.mahfooz.spark.orc.zstandard

import org.apache.spark.sql.SparkSession

object ZStandard {

  def main(args: Array[String]): Unit = {

    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse")
    val sparkMetastoreDb = sys.env.getOrElse("derby.system.home", "")
    System.setProperty("derby.system.home", sparkMetastoreDb)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .config("spark.sql.orc.impl", "native")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    spark.sql(
      """
        |CREATE TABLE if not exists compressed (
        |  key STRING,
        |  value STRING
        |)
        |USING ORC
        |OPTIONS (
        |  compression 'zstd'
        |)
        |""".stripMargin)
  }

}
