package com.mahfooz.schema.evolution.removing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RemovingColumnSchema {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("RemovingColumnSchema")
                .master("local")
                .getOrCreate();

        String schema = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"User\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"id\", \"type\": \"int\"},\n" +
                "    {\"name\": \"name\", \"type\": \"string\"},\n" +
                "    {\"name\": \"age\", \"type\": \"long\", \"deprecated\": true}\n" +
                "  ]\n" +
                "}\n";

        // Read Avro data with the latest schema
        String dataHome = System.getenv("DATA_HOME");
        Dataset<Row> avroDataFrame = spark.read().format("avro").option("avroSchema", schema)
                .load(String.format("%s/FileData/Avro/schema/user.avro",dataHome));

        // Show the content of the DataFrame
        avroDataFrame.show();

        // Stop SparkSession
        spark.stop();
    }
}
