package com.mahfooz.parquet.vectorization;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class EnableVectorization {

    public static void main(String[] args) {
        // Create a SparkConf and set the application name
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("EnableVectorization");

        // Enable vectorized Parquet reader
        sparkConf.set("spark.sql.parquet.enableVectorizedReader", "true");

        // Create a SparkSession with the SparkConf
        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        String dataHome = System.getenv("DATA_HOME");
        String outputPath = String.format("%s/FileData/Parquet/reported-crimes", dataHome);

        // Sample data for demonstration
        Dataset<Row> data = spark.read().parquet(outputPath);

        data.show();

        // Stop the SparkSession when done
        spark.stop();
    }
}
