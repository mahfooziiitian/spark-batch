package com.mahfooz.dataframe.partition.repartition.range

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object RepartitionByRange {

  val dataHome  = sys.env.getOrElse("DATA_HOME","data")+"\\spark\\partition"
  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("RepartitionByRange")
      .getOrCreate()

    val data = Seq((1,10),(2,20),(3,10),(4,20),(5,10),
      (6,30),(7,50),(8,50),(9,50),(10,30),
      (11,10),(12,10),(13,40),(14,40),(15,40),
      (16,40),(17,50),(18,10),(19,40),(20,40)
    )

    import spark.sqlContext.implicits._
    val dfRange = data.toDF("id","count")
      .repartitionByRange(5,col("count"))

    dfRange.write.option("header",true)
      .csv(s"${dataHome}\\range.csv")
  }

}
