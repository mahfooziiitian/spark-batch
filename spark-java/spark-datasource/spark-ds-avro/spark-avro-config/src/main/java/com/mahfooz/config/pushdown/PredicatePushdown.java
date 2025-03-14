package com.mahfooz.config.pushdown;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PredicatePushdown {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("PredicatePushdown")
                .master("local")
                .getOrCreate();

        String dataHome = System.getenv("DATA_HOME");
        String inputPath = String.format("%s/FileData/Avro/schema/users.avro", dataHome);

        // Read Avro data into a DataFrame
        Dataset<Row> avroDataFrame = spark.read()
                .format("avro")
                .load(inputPath);

        // Perform filtering after data has been loaded
        Dataset<Row> filteredDataFrame = avroDataFrame.filter("age > 25");

        // Show the content of the filtered DataFrame
        filteredDataFrame.show();

        filteredDataFrame.explain();

        // Stop SparkSession
        spark.stop();
    }
}
