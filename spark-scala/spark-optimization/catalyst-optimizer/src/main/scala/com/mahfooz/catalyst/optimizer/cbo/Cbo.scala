/**
 * Cost-Based Optimization (aka Cost-Based Query Optimization or CBO Optimizer) is
 * an optimization technique in Spark SQL that uses table statistics to determine the
 * most efficient query execution plan of a structured query (given the logical query plan).
 *
 * Cost-based optimization is disabled by default. Spark SQL uses spark.sql.cbo.enabled configuration property to control
 * whether the CBO should be enabled and used for query optimization or not.
 *
 * You first use ANALYZE TABLE COMPUTE STATISTICS SQL command to compute table statistics.
 *
 * Use DESCRIBE EXTENDED SQL command to inspect the statistics.
 *
 */
package com.mahfooz.catalyst.optimizer.cbo

import org.apache.spark.sql.SparkSession

object Cbo {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("CatalogMetastore")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    val sqlConf = spark.sessionState.conf
    println(sqlConf.cboEnabled)

    val sparkCboEnabled = spark.newSession
    import org.apache.spark.sql.internal.SQLConf.CBO_ENABLED
    sparkCboEnabled.conf.set(CBO_ENABLED.key, true)
    val isCboEnabled = sparkCboEnabled.conf.get(CBO_ENABLED.key)
    println(s"Is CBO enabled? $isCboEnabled")
    println(spark.sessionState.conf.histogramEnabled)

    import spark.implicits._

    val tableName = "t1"

    // Make the example reproducible
    import org.apache.spark.sql.catalyst.TableIdentifier
    val tid = TableIdentifier(tableName)
    val sessionCatalog = spark.sessionState.catalog
    sessionCatalog.dropTable(tid, ignoreIfNotExists = true, purge = true)

    spark.sql(s"drop table if exists ${tableName}")

    // CREATE TABLE t1
    Seq((0, 0, "zero"), (1, 1, "one")).
      toDF("id", "p1", "p2").
      write.
      saveAsTable(tableName)

    // As we drop and create immediately we may face problems with unavailable partition files
    // Invalidate cache
    spark.sql(s"REFRESH TABLE $tableName")

    // Use ANALYZE TABLE...FOR COLUMNS to compute column statistics
    // that saves them in a metastore (aka an external catalog)
    val df = spark.table(tableName)
    val allCols = df.columns.mkString(",")
    val analyzeTableSQL = s"ANALYZE TABLE t1 COMPUTE STATISTICS FOR COLUMNS $allCols"
    spark.sql(analyzeTableSQL)

    val colName = "id"
    val descExtSQL = s"DESC EXTENDED $tableName $colName"

    // 254 bins by default --> num_of_bins in histogram row below
   spark.sql(descExtSQL).show(truncate = false)

  }

}
