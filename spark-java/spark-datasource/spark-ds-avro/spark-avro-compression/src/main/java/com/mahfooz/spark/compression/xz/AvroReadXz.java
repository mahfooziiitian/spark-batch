package com.mahfooz.spark.compression.xz;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AvroReadXz {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("AvroReadXz")
                .master("local")
                .getOrCreate();
        String dataHome = System.getenv("DATA_HOME");
        String inputPath = String.format("%s/FileData/Avro/compression/users_xz", dataHome);
        String schema = "{ \"type\": \"record\", \"name\": \"User\", \"fields\": [ {\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"name\", \"type\": \"string\"}, {\"name\": \"age\", \"type\": \"long\"} ]}";
        Dataset<Row> df = spark.read().format("avro")
                .option("avroSchema", schema)
                .option("compression", "xz")
                .load(inputPath);

        df.show(false);

        // Stop SparkSession
        spark.stop();
    }
}
