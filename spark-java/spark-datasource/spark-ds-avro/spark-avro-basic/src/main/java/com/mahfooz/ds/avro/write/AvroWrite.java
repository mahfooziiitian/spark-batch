package com.mahfooz.ds.avro.write;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AvroWrite {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AvroWrite").setMaster("local[*]");
        try(SparkSession spark = SparkSession.builder().config(conf).getOrCreate();) {
            String dataHome = System.getenv("DATA_HOME");
            String outputPath = String.format("%s/FileData/Avro/weather_write.avro", dataHome);
            String inputPath = String.format("%s/FileData/Avro/weather.avro", dataHome);
            Dataset<Row> df = spark.read().format("avro").load(inputPath);
            df.write().format("avro").mode("overwrite").save(outputPath);
        }
    }
}
