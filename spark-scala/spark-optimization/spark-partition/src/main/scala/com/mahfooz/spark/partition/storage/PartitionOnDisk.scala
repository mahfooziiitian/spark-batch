package com.mahfooz.spark.partition.storage

import org.apache.spark.sql.SparkSession

object PartitionOnDisk {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("partition-disk")
      .master("local[*]")
      .getOrCreate()

    val driverData = Seq(
      ("Alice", "A224455", "Female", 3000),
      ("Bryan","B992244","Male",4000),
      ("Catherine","C887733","Female",2000),
      ("Daryl","D229988","Male",3000),
      ("Jenny","J663300","Female", 6000))

    import spark.implicits._
    val df = driverData.toDF("name","license","gender","salary")

    val dataHome = sys.env.getOrElse("DATA_HOME","data")

    df.write.partitionBy("gender","salary")
      .parquet(s"${dataHome}\\spark\\partition\\disk\\people")
  }
}
