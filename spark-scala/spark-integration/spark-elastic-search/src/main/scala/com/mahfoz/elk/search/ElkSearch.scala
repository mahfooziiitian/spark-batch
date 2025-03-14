package com.mahfoz.elk.search

import org.apache.spark.sql.SparkSession

case class AlbumIndex(artist:String, yearOfRelease:Int, albumName: String)

object ElkSearch {

  def main(args: Array[String]): Unit = {
    ElkSearch.writeToIndex()
  }

  private def writeToIndex(): Unit = {

    val spark = SparkSession
      .builder()
      .appName("WriteToES")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val dfReader = spark.read.format("org.elasticsearch.spark.sql")
      .option("es.read.metadata", "true")
      .option("es.nodes.wan.only","true")
      .option("es.index.auto.create", "true")
      .option("es.port","9200")
      .option("es.net.ssl","false")
      .option("es.nodes", "http://localhost")
      .option("es.resource","megacorp")

    val df = dfReader.load("megacorp")
    df.show()

    //df.filter(col("megacorp") === "Harvard").show()

  }

}
