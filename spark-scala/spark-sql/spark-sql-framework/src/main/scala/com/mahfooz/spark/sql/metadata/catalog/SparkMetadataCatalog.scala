package com.mahfooz.spark.sql.metadata.catalog

import org.apache.spark.sql.SparkSession

object SparkMetadataCatalog {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkMetadataCatalog")
      .config("spark.sql.warehouse.dir","D:\\data\\processing\\spark-warehouse")
      .getOrCreate()

    spark.catalog.listTables.show

    //now register the movies data frame
    val movies=spark.read.json("D:\\data\\movies\\movies.json")

    movies.createOrReplaceTempView("movies")

    spark.catalog.listTables.show

    // show the list of columns of movies view in catalog
    spark.catalog.listColumns("movies").show

    // register movies as global temporary view called movies_g
    movies.createOrReplaceGlobalTempView("movies_g")
    spark.catalog.listTables.show
  }

}
