package com.mahfooz.df.aggregation.cube;

import com.mahfooz.df.aggregation.util.DownloadDataFile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

class CubeDF {

  static String warehouseLocation = System.getenv("SPARK_WAREHOUSE");
  static String dataHome = System.getenv("DATA_HOME")+"\\FileData\\Csv";

  private static SparkSession start(){
    return SparkSession
      .builder()
      .master("local[*]")
      .appName("CubeDF")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .getOrCreate();
  }

  public static void main(String[] args) {
      Dataset<Row> statesPopulationDF;
      try (SparkSession spark = start()) {
          String filename = "statesPopulation.csv";
          String downloadFileUrl = "https://raw.githubusercontent.com/PacktPublishing/Scala-and-Spark-for-Big-Data-Analytics/master/data/data/statesPopulation.csv";

          if (!new File(dataHome + "/" + filename).exists()) {
              DownloadDataFile.fileDownloader(downloadFileUrl, dataHome + "/" + filename);
          }

          String dataPath = "file:///" + (dataHome + "/" + filename).replace("\\", "/");

          statesPopulationDF = spark.read()
                  .option("header", "true")
                  .option("inferschema", "true")
                  .csv(dataPath);
        statesPopulationDF.limit(30)
                .cube("State", "Year")
                .count()
                .orderBy("year")
                .show(100);
      }
  }
}
