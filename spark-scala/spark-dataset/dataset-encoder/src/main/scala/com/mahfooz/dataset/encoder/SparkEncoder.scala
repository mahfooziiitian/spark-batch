package com.mahfooz.dataset.encoder

import com.mahfooz.spark.dataset.model.Client
import org.apache.spark.sql.SparkSession

object SparkEncoder {

  def main(args: Array[String]): Unit = {

    val sparkWarehouse =
      sys.env
        .getOrElse("SPARK_WAREHOUSE", "spark-warehouse")
        .replace("\\", "/")

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkHiveDatabase")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    val jsoFilePath =
      s"file:///${sys.env
        .getOrElse("DATA_HOME", "data")
        .replace("\\", "/")}/FileData/Json/client.json"

    import spark.implicits._

    val ds = spark.read
      .json(jsoFilePath)
      .as[Client]

    ds.printSchema()

    val dsNew = ds
      .filter(client => client.age > 18)
      .map(client => (client.age, client.countryCode))
      .groupBy($"_2")
      .avg()

    dsNew.show()
    spark.stop()

  }
}
