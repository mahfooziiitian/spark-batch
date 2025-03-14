package com.mahfooz.spark.compression.xz;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AvroWriteXz {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("AvroWriteXz")
                .master("local")
                .config("spark.sql.avro.compression.codec","bzip2")
                .getOrCreate();
        String dataHome = System.getenv("DATA_HOME");
        String inputPath = String.format("%s/FileData/Avro/schema/users.avro", dataHome);
        String schema = "{ \"type\": \"record\", \"name\": \"User\", \"fields\": [ {\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"name\", \"type\": \"string\"}, {\"name\": \"age\", \"type\": \"long\"} ]}";
        Dataset<Row> df = spark.read().format("avro")
                .option("avroSchema", schema)
                .load(inputPath);

        // Specify Avro options
        String outputPath = String.format("%s/FileData/Avro/compression/users_xz", dataHome);
        df.write()
                .format("avro")
                .mode("overwrite")
                .option("avroSchema", schema)
                .option("compression", "xz")
                .save(outputPath);

        // Stop SparkSession
        spark.stop();
    }
}
