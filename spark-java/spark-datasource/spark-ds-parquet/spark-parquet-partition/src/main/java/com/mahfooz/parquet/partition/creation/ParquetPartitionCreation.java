package com.mahfooz.parquet.partition.creation;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class ParquetPartitionCreation {

    public static void main(String[] args) {

        // Create a SparkConf and set the application name
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("ParquetPartitionCreation");

        // Create a SparkSession with the SparkConf
        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
        String dataHome = System.getenv("DATA_HOME");
        String inputPath = String.format("%s/FileData/Csv/OnlineRetail.csv", dataHome);
        String outputPath = String.format("%s/FileData/Parquet/OnlineRetail", dataHome);

        // Sample data for demonstration
        Dataset<Row> data = spark.read().format("csv").option("header", "true").load(inputPath)
                .withColumn("InvoiceDateOnly", col("InvoiceDate").cast("date"));

        // Define the columns for partitioning
        String[] partitionColumns = {"Country"};

        // Write data to Parquet files with partitioning
        data.write()
                .mode(SaveMode.Overwrite)
                .partitionBy(partitionColumns)
                .parquet(outputPath);

        // Stop the SparkSession when done
        spark.stop();
    }
}
