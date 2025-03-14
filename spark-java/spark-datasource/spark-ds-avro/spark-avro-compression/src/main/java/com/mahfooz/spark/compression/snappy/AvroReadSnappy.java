package com.mahfooz.spark.compression.snappy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AvroReadSnappy {

        public static void main(String[] args) {

            SparkSession spark = SparkSession.builder()
                    .appName("AvroReadSnappy")
                    .master("local")
                    .getOrCreate();
            String dataHome = System.getenv("DATA_HOME");
            String inputPath = String.format("%s/FileData/Avro/compression/users", dataHome);
            String schema = "{ \"type\": \"record\", \"name\": \"User\", \"fields\": [ {\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"name\", \"type\": \"string\"}, {\"name\": \"age\", \"type\": \"long\"} ]}";
            Dataset<Row> df = spark.read().format("avro")
                    .option("avroSchema", schema)
                    .option("compression", "snappy")
                    .load(inputPath);

           df.show(false);
            // Stop SparkSession
            spark.stop();
        }
    }
