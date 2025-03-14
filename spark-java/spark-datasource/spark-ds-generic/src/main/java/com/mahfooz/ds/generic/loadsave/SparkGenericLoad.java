package com.mahfooz.ds.generic.loadsave;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkGenericLoad {
    public static void main(String[] args) {
        
        SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .appName("SparkGenericLoad")
        .getOrCreate();

        Dataset<Row> usersDF = spark.read()
        .load("d:/data/spark/parquet/users.parquet");
        usersDF.select("name", "favorite_color")
        .write()
        .save("d:/data/spark/parquet/namesAndFavColors.parquet");
        
        spark.stop();
    }
}
