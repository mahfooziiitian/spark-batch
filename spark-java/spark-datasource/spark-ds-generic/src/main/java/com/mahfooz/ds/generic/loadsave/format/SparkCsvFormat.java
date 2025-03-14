package com.mahfooz.ds.generic.loadsave.format;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkCsvFormat {
    public static void main(String[] args) {
        
        SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .appName("SparkCsvFormat")
        .getOrCreate();
        
        Dataset<Row> peopleDFCsv = spark.read()
        .format("csv")
        .option("sep", ";")
        .option("inferSchema", "true")
        .option("header", "true")
        .load("d:/data/spark/csv/people.csv"); 

        peopleDFCsv.write()
        .format("csv")
        .save("d:/data/spark/csv/people_out.csv");

        spark.stop();
    }
}
