/**
 * CatalogStatistics are table statistics that are stored in an external catalog (aka metastore).
 * Physical total size (in bytes)
 * Estimated number of rows (aka row count)
 * Column statistics (i.e. column names and their statistics)
 *
 */
package com.mahfooz.spark.stats.externalcatalog

import org.apache.spark.sql.SparkSession

object ExternalCatalogStats {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ExternalCatalogStats")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    val db = spark.catalog.currentDatabase
    val tableName = "t1"
    val metadata = spark.sharedState.externalCatalog.getTable(db, tableName)
    val stats = metadata.stats

    val tid = spark.sessionState.sqlParser.parseTableIdentifier(tableName)
    val metadataView = spark.sessionState.catalog.getTempViewOrPermanentTableMetadata(tid)
    val statsView = metadataView.stats

  }

}
