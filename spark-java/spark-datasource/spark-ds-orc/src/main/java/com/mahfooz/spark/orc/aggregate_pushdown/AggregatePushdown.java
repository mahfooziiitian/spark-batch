/**
 * SparkORCAggregatePushdown
 */
package com.mahfooz.spark.orc.aggregate_pushdown;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AggregatePushdown {

    public static void main(String[] args) {
        // Create a SparkConf and set the application name
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("AggregatePushdown");

        // Create a SparkSession with the SparkConf
        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        // Enable or disable aggregate pushdown based on your requirement
        spark.conf().set("spark.sql.orc.aggregatePushdown", "true");

        // Your Spark application logic goes here
        String orcInputPath = String.format("%s/FileData/Orc/demo-11-zlib.orc", System.getenv("DATA_HOME"));

        Dataset<Row> df = spark.read().format("orc")
                .load(orcInputPath);

        df.printSchema();

        // Stop the SparkSession when done
        spark.stop();
    }
}
