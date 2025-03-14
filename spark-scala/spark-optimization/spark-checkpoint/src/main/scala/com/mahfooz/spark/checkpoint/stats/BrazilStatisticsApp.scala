package com.mahfooz.spark.checkpoint.stats

import com.mahfooz.dataframe.util.download.DownloadDataFile
import com.mahfooz.dataframe.util.path.FilePathUtil
import com.mahfooz.spark.checkpoint.util.Mode
import com.mahfooz.spark.checkpoint.util.Mode._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.io.File

object BrazilStatisticsApp {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse")
  val dataHome = sys.env.getOrElse("DATA_HOME", "") + "/spark/datasource/csv"

  def main(args: Array[String]): Unit = {
    start()
  }

  private def start(): Unit = {

    val spark = SparkSession.builder()
      .appName("Brazil economy")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate();

    val sc = spark.sparkContext
    sc.setCheckpointDir(sys.env.getOrElse("DATA_HOME", "data") + "\\spark\\checkpoint")

    val filename = "BRAZIL_CITIES.csv"
    val downloadFileUrl = "https://raw.githubusercontent.com/mahfooz-code/data/main/BRAZIL_CITIES.csv"

    if (!new File(s"$dataHome/$filename").exists()) {
      DownloadDataFile.fileDownloader(downloadFileUrl, s"${dataHome}/${filename}")
    }

    val dataPath = "file:///" + FilePathUtil.windowToUnixPath(s"$dataHome")

    val df = spark.read.format("csv")
      .option("header", true)
      .option("sep", ";")
      .option("enforceSchema", true)
      .option("nullValue", "null")
      .option("inferSchema", true)
      .load(s"${dataPath}/${filename}")

    println("***** Raw dataset and schema")
    df.show(100)
    df.printSchema()

    val t0 = process(df, Mode.NO_CACHE_NO_CHECKPOINT)
    val t1 = process(df, Mode.CACHE)
    val t2 = process(df, Mode.CHECKPOINT)
    val t3 = process(df, Mode.CHECKPOINT_NON_EAGER)

    println("\n***** Processing times (excluding purification)")
    println("Without cache ............... " + t0 + " ms")
    println("With cache .................. " + t1 + " ms")
    println("With checkpoint ............. " + t2 + " ms")
    println("With non-eager checkpoint ... " + t3 + " ms")
  }

  def process(df: Dataset[Row], mode: Mode.Value): Long = {
    val t0 = System.currentTimeMillis();
    var dfAggregate = df.orderBy(col("CAPITAL").desc)
      .withColumn("WAL_MART",
        when(col("WAL_MART").isNull, 0)
          .otherwise(col("WAL_MART")))
      .withColumn("MAC",
        when(col("MAC").isNull, 0).otherwise(col("MAC")))
      .withColumn("GDP", regexp_replace(col("GDP"), ","
        , "."))
      .withColumn("GDP", col("GDP").cast("float"))
      .withColumn("area", regexp_replace(col("area"), ",",
        ""))
      .withColumn("area", col("area").cast("float"))
      .groupBy("STATE")
      .agg(
        first("CITY").as("capital"),
        sum("IBGE_RES_POP_BRAS").as("pop_brazil"),
        sum("IBGE_RES_POP_ESTR").as("pop_foreign"),
        sum("POP_GDP").as("pop_2016"),
        sum("GDP").as("gdp_2016"),
        sum("POST_OFFICES").as("post_offices_ct"),
        sum("WAL_MART").as("wal_mart_ct"),
        sum("MAC").as("mc_donalds_ct"),
        sum("Cars").as("cars_ct"),
        sum("Motorcycles").as("moto_ct"),
        sum("AREA").as("area"),
        sum("IBGE_PLANTED_AREA").as("agr_area"),
        sum("IBGE_CROP_PRODUCTION_$").as("agr_prod"),
        sum("HOTELS").as("hotels_ct"),
        sum("BEDS").as("beds_ct"))
      .withColumn("agr_area", expr("agr_area / 100"))
      .orderBy(col("STATE"))
      .withColumn("gdp_capita", expr("gdp_2016 / pop_2016 * 1000"))

    mode match {
      case CACHE => dfAggregate = dfAggregate.cache()
      case CHECKPOINT => dfAggregate = dfAggregate.checkpoint()
      case CHECKPOINT_NON_EAGER => dfAggregate = dfAggregate.checkpoint(false)
      case _ =>
    }
    System.out.println("***** Pure data")
    df.show(5)
    val t1 = System.currentTimeMillis()
    System.out.println("Aggregation (ms) .................. " + (t1 - t0))

    // Regions per population
    println("***** Population")
    val popDf = dfAggregate.drop("area", "pop_brazil", "pop_foreign",
      "post_offices_ct", "cars_ct", "moto_ct", "mc_donalds_ct",
      "agr_area", "agr_prod", "wal_mart_ct", "hotels_ct", "beds_ct",
      "gdp_capita", "agr_area", "gdp_2016")
      .orderBy(col("pop_2016").desc)
    popDf.show(30)
    val t2 = System.currentTimeMillis
    println("Population (ms) ................... " + (t2 - t1))

    // Regions per size in km2
    println("***** Area (squared kilometers)")
    val areaDf = dfAggregate.withColumn("area", round(col("area"), 2))
      .drop("pop_2016", "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct", "moto_ct", "mc_donalds_ct", "agr_area", "agr_prod", "wal_mart_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016")
      .orderBy(col("area").desc)
    areaDf.show(30)
    val t3 = System.currentTimeMillis
    println("Area (ms) ......................... " + (t3 - t2))

    // McDonald's per 1m inhabitants
    println("***** McDonald's restaurants per 1m inhabitants")
    val mcDonaldPopDf = dfAggregate.withColumn("mcd_1m_inh", expr("int(mc_donalds_ct / pop_2016 * 100000000) / 100")).drop("pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct", "moto_ct", "area", "agr_area", "agr_prod", "wal_mart_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016").orderBy(col("mcd_1m_inh").desc)
    mcDonaldPopDf.show(5)
    val t4 = System.currentTimeMillis
    println("Mc Donald's (ms) .................. " + (t4 - t3))

    // Walmart per 1m inhabitants
    println("***** Walmart supermarket per 1m inhabitants")
    val walmartPopDf = dfAggregate.withColumn("walmart_1m_inh", expr("int(wal_mart_ct / pop_2016 * 100000000) / 100")).drop("pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct", "moto_ct", "area", "agr_area", "agr_prod", "mc_donalds_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016").orderBy(col("walmart_1m_inh").desc)
    walmartPopDf.show(5)
    val t5 = System.currentTimeMillis
    println("Walmart (ms) ...................... " + (t5 - t4))

    // GDP per capita
    println("***** GDP per capita")
    val gdpPerCapitaDf = dfAggregate.drop("pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct", "moto_ct", "area", "agr_area", "agr_prod", "mc_donalds_ct", "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area").withColumn("gdp_capita", expr("int(gdp_capita)")).orderBy(col("gdp_capita").desc)
    gdpPerCapitaDf.show(5)
    val t6 = System.currentTimeMillis
    println("GDP per capita (ms) ............... " + (t6 - t5))

    // Post offices
    println("***** Post offices")
    var postOfficeDf = dfAggregate
      .withColumn("post_office_1m_inh", expr("int(post_offices_ct / pop_2016 * 100000000) / 100"))
      .withColumn("post_office_100k_km2", expr("int(post_offices_ct / area * 10000000) / 100"))
      .drop("gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita", "cars_ct", "moto_ct", "agr_area", "agr_prod", "mc_donalds_ct", "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area", "pop_brazil")
      .orderBy(col("post_office_1m_inh").desc)

    mode match {
      case CACHE =>
        postOfficeDf = postOfficeDf.cache

      case CHECKPOINT =>
        postOfficeDf = postOfficeDf.checkpoint

      case CHECKPOINT_NON_EAGER =>
        postOfficeDf = postOfficeDf.checkpoint(false)

      case _ =>

    }
    println("****  Per 1 million inhabitants")
    val postOfficePopDf = postOfficeDf.drop("post_office_100k_km2", "area").orderBy(col("post_office_1m_inh").desc)
    postOfficePopDf.show(5)
    println("****  per 100000 km2")
    val postOfficeArea = postOfficeDf.drop("post_office_1m_inh", "pop_2016").orderBy(col("post_office_100k_km2").desc)
    postOfficeArea.show(5)
    val t7 = System.currentTimeMillis
    println("Post offices (ms) ................. " + (t7 - t6) + " / Mode: " + mode)

    // Cars and motorcycles per 1k habitants
    System.out.println("***** Vehicles")
    val vehiclesDf = dfAggregate.withColumn("veh_1k_inh", expr("int((cars_ct + moto_ct) / pop_2016 * 100000) / 100")).drop("gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita", "post_offices_ct", "agr_area", "agr_prod", "mc_donalds_ct", "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area", "area", "pop_brazil").orderBy(col("veh_1k_inh").desc)
    vehiclesDf.show(5)
    val t8 = System.currentTimeMillis
    println("Vehicles (ms) ..................... " + (t8 - t7))


    println("***** Agriculture - usage of land for agriculture")
    val agricultureDf = dfAggregate.withColumn("agr_area_pct", expr("int(agr_area / area * 1000) / 10")).withColumn("area", expr("int(area)")).drop("gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita", "post_offices_ct", "moto_ct", "cars_ct", "mc_donalds_ct", "hotels_ct", "beds_ct", "wal_mart_ct", "pop_brazil", "agr_prod", "pop_2016").orderBy(col("agr_area_pct").desc)
    agricultureDf.show(5)
    val t9 = System.currentTimeMillis
    println("Agriculture revenue (ms) .......... " + (t9 - t8))

    val tf = System.currentTimeMillis
    System.out.println("Total with purification (ms) ...... " + (tf - t0))
    System.out.println("Total without purification (ms) ... " + (tf - t0))
    tf - t0
  }

}
