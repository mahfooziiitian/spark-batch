package com.mahfooz.df.aggregation.basic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.approx_count_distinct;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;

public class SparkCountAggregation {

    public static void main(String[] args) {

    SparkSession spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkCountAggregation")
      .getOrCreate();

    String dataHome = System.getenv("DATA_HOME");

    Dataset<Row> flight_summary = spark.read()
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(dataHome+"\\FileData\\Csv\\Flight\\flights.csv");

    flight_summary.select(countDistinct("origin_airport"),
      countDistinct("destination_airport"),
      count("*")).show();

    flight_summary.select(count("origin_airport"),
            countDistinct("destination_airport"),
      approx_count_distinct("origin_airport", 0.05)).show();

  }
}
