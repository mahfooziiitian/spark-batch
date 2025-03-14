package com.mahfooz.schema.evolution;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSchemaEvolution {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkSchemaEvolution")
                .master("local")
                .getOrCreate();

        // Specify the latest schema
        String latestSchema = "{ \"type\": \"record\", \"name\": \"User\", \"fields\": [ {\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"full_name\", \"type\": \"string\", \"aliases\": [\"name\"]}, {\"name\": \"age\", \"type\": \"long\"} ]}";

        // Read Avro data with the latest schema
        String dataHome = System.getenv("DATA_HOME");
        Dataset<Row> avroDataFrame = spark.read().format("avro").option("avroSchema", latestSchema)
                .load(String.format("%s/FileData/Avro/schema/user.avro",dataHome));

        // Show the content of the DataFrame
        avroDataFrame.show();

        // Stop SparkSession
        spark.stop();
    }
}
