package com.mahfooz.parquet.encryption;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class ColumnarEncryption {

    public static void main(String[] args) {
        // Create a SparkConf and set the application name
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("ColumnarEncryption");

        // Create a SparkSession with the SparkConf
        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        // Sample data for demonstration
        String inputPath = String.format("%s/FileData/Json/Movies/movies.json",System.getenv("DATA_HOME"));
        Dataset<Row> data = spark.read().json(inputPath);

        data.printSchema();

        // Define a user-defined function (UDF) for encryption
        UDF1<String, String> encryptUDF = input -> Base64.getEncoder().encodeToString(input.getBytes(StandardCharsets.UTF_8));

        // Register the UDF for encryption
        spark.udf().register("encrypt", encryptUDF, DataTypes.StringType);

        // Apply the encryption UDF to a specific column
        Dataset<Row> encryptedData = data.withColumn("actor_name_encrypted", functions.callUDF("encrypt", data.col("actor_name")));

        // Show the encrypted data
        encryptedData.show();

        // Stop the SparkSession when done
        spark.stop();
    }
}
