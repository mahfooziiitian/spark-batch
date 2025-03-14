/*
Catalog is the interface for managing a metastore (aka metadata catalog) of relational entities (e.g. database(s), tables, functions, table columns and temporary views).

Catalog is available using SparkSession.catalog property.


 */
package com.mahfooz.spark.framework.catalog

import org.apache.spark.sql.SparkSession

object SparkCatalog {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("catalog")
      .master("local[*]")
      .getOrCreate()

  }
}
