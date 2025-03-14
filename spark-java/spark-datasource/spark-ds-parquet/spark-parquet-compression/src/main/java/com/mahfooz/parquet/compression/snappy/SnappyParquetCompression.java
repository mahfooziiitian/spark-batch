package com.mahfooz.parquet.compression.snappy;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SnappyParquetCompression {

    public static void main(String[] args) {
        // Create a SparkConf and set the application name
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SnappyParquetCompression");

        // Create a SparkSession with the SparkConf
        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
        String dataHome = System.getenv("DATA_HOME");
        String inputPath = String.format("%s/FileData/Csv/reported-crimes.csv", dataHome);
        String outputPath = String.format("%s/FileData/Parquet/reported-crimes", dataHome);

        // Sample data for demonstration
        Dataset<Row> data = spark.read().format("csv").load(inputPath);

        // Configure Snappy compression when writing Parquet files
        data.write()
                .option("compression", "snappy")
                .parquet(outputPath);

        // Stop the SparkSession when done
        spark.stop();
    }
}
