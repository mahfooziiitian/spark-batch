package com.mahfooz.spark.hive.local

import com.mahfooz.spark.hive.common.SparkHiveCommon
import org.apache.spark.sql.Row

object HiveLocal {

  def main(args: Array[String]): Unit = {

    val spark = SparkHiveCommon.createSparkSession("HiveLocal")

    import spark.implicits._
    import spark.sql

    // Aggregation queries are also supported.
    sql("SELECT COUNT(*) FROM src").show()

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    // The items in DataFrames are of type Row, which allows you to access each column by ordinal.
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }

    stringsDS.show()

    spark.stop()
  }
}
