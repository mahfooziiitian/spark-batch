package com.mahfooz.df.aggregation.config;

import org.apache.spark.sql.SparkSession;

public class RetainColumnGroupEx {

  static String warehouseLocation =System.getenv("SPARK_WAREHOUSE");
  static String dataHome = System.getenv("DATA_HOME")+"\\spark\\datasource\\csv";

  private static SparkSession start(){
    return SparkSession
      .builder()
      .master("local[*]")
      .appName("RollupDF")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .getOrCreate();
  }

  public static void main(String[] args) {
      try (SparkSession spark = start()) {
          System.out.println(spark.conf().get("spark.sql.retainGroupColumns"));
          System.out.println(spark.sessionState().conf().dataFrameRetainGroupColumns());
      }
  }

}
