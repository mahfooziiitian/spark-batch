package com.mahfooz.ds.avro.read;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.MessageFormat;

public class AvroReading {

        public static void main(String[] args) {
            SparkConf conf = new SparkConf().setAppName("AvroReading").setMaster("local[*]");
            String dataHome = System.getenv("DATA_HOME");

            try(JavaSparkContext sc = new JavaSparkContext(conf);
                SparkSession spark = SparkSession.builder().config(conf).getOrCreate()) {
                // Read Avro data into a DataFrame
                String inputPath = String.format("%s/FileData/Avro/weather.avro", dataHome);
                Dataset<Row> df = spark.read().format("avro").load(inputPath);

                // Show the content of the DataFrame
                df.show();
            }
        }

}
