package com.mahfooz.spark.stats.column

import org.apache.spark.sql.SparkSession

object ColumnStats {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")
  System.setProperty("derby.system.home", sys.env.getOrElse("derby.system.home",""))

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ColumnStats")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    val tableName = "t1"

    // Make the example reproducible
    import org.apache.spark.sql.catalyst.TableIdentifier
    val tid = TableIdentifier(tableName)
    val sessionCatalog = spark.sessionState.catalog
    sessionCatalog.dropTable(tid, ignoreIfNotExists = true, purge = true)

    import spark.implicits._

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

    // Fetch the table metadata (with column statistics) from a metastore
    val metastore = spark.sharedState.externalCatalog
    val db = spark.catalog.currentDatabase
    val tableMeta = metastore.getTable(db, table = tableName)

    // The column statistics are part of the table statistics
    val colStats = tableMeta.stats.get.colStats
    colStats.map { case (name, cs) => s"$name: $cs" }.foreach(println)

  }

}
