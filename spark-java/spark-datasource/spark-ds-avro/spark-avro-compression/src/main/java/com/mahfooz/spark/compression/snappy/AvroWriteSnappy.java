package com.mahfooz.spark.compression.snappy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AvroWriteSnappy {

        public static void main(String[] args) {

            SparkSession spark = SparkSession.builder()
                    .appName("AvroWriteSnappy")
                    .master("local")
                    .getOrCreate();
            String dataHome = System.getenv("DATA_HOME");
            String inputPath = String.format("%s/FileData/Avro/schema/users.avro", dataHome);
            String schema = "{ \"type\": \"record\", \"name\": \"User\", \"fields\": [ {\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"name\", \"type\": \"string\"}, {\"name\": \"age\", \"type\": \"long\"} ]}";
            Dataset<Row> df = spark.read().format("avro").load(inputPath);

            // Specify Avro options
            String outputPath = String.format("%s/FileData/Avro/compression/users", dataHome);
            df.write()
                    .format("avro")
                    .mode("overwrite")
                    .option("avroSchema", schema)
                    .option("compression", "snappy")
                    .save(outputPath);

            // Stop SparkSession
            spark.stop();
        }
    }
