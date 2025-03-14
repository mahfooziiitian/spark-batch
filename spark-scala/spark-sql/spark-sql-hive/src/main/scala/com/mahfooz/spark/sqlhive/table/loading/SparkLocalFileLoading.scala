package com.mahfooz.spark.sqlhive.table.loading


import com.mahfooz.spark.sqlhive.util.Record
import org.apache.spark.sql.{Row, SparkSession}

object SparkLocalFileLoading {

  def main(args: Array[String]): Unit = {

    // warehouseLocation points to the default location for
    // managed databases and tables
    val warehouseLocation = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")
    val derbySystemHome = sys.env.getOrElse("derby.system.home","metastore_db")
    val dataDirHome = sys.env.getOrElse("DATA_HOME","data")+"\\spark\\textfile"

    println(s"warehouseLocation : $warehouseLocation")
    println(s"derby home: $derbySystemHome")

    System.setProperty("derby.system.home",derbySystemHome)

    val spark = SparkSession.builder
      .appName("local file loading spark hive table")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .master("local[*]")
      .enableHiveSupport
      .getOrCreate

    import spark.implicits._
    import spark.sql

    val dataPath = s"${sys.env.getOrElse("DATA_HOME","data")}/FileData/Text/kv1.txt"

    sql("CREATE TABLE IF NOT EXISTS file_load (key INT, value STRING) USING hive")
    sql(s"LOAD DATA LOCAL INPATH '${dataPath}' INTO TABLE file_load")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM file_load").show

    // Aggregation queries are also supported.
    sql("SELECT COUNT(*) FROM file_load").show

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF = sql("SELECT key, value FROM file_load WHERE key < 10 ORDER BY key")

    // The items in DataFrames are of type Row, which allows you to access
    // each column by ordinal
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }

    stringsDS.show

    // You can also use DataFrames to create temporary views within a SparkSession.
    val recordsDF =
      spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")

    // Queries can then join DataFrame data with data stored in Hive.
    sql("SELECT * FROM records r JOIN file_load s ON r.key = s.key").show

    sql("drop table file_load")

  }
}
