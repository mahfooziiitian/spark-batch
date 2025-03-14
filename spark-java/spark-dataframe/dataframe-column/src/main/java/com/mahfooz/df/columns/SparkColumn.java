package com.mahfooz.df.columns;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;

public class SparkColumn {

    public static void main(String[] args) {

    SparkSession spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkBasicAggregation")
      .getOrCreate();

    String dataHome = System.getenv("DATA_HOME");

    Dataset<Row> df = spark.read()
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(dataHome+"\\FileData\\Csv\\Retail\\all\\online-retail-dataset.csv");

    for(String column: df.columns())
        System.out.print(column+" ");
    System.out.println();

    df.select(count("StockCode")).show();

    df.select(countDistinct("StockCode")).show();

  }

}
