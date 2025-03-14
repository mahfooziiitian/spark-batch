package com.mahfooz.parquet.partition.discovery;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PartitionDiscovery {

    public static void main(String[] args) {

        // Create a SparkConf and set the application name
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("PartitionDiscovery");


        // Create a SparkSession with the SparkConf
        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        System.out.println(spark.conf().get("spark.sql.sources.partitionColumnTypeInference.enabled"));

        String dataHome = System.getenv("DATA_HOME");
        String inputPath = String.format("%s/FileData/Parquet/OnlineRetail", dataHome);

        // Sample data for demonstration
        Dataset<Row> data = spark.read().format("parquet").load(inputPath);

        data.printSchema();

        data.show();

        // Stop the SparkSession when done
        spark.stop();
    }
}
