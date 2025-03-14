package com.mahfooz.ds.avro;

import com.mahfooz.ds.avro.util.DownloadFile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

public class AvroToDataframeApp {

    /**
     * main() is your entry point to the application.
     *
     */
    public static void main(String[] args) {
        AvroToDataframeApp app = new AvroToDataframeApp();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Avro to Dataframe")
                .master("local[*]")
                .getOrCreate();

        String dataHome = System.getenv("DATA_HOME") + "\\FileData\\Avro";
        String downloadUrl = "https://raw.githubusercontent.com/jgperrin/net.jgp.books.spark.ch07/master/data/weather.avro";
        String filePath = dataHome + "\\weather.avro";
        if (!(new File(filePath).exists()))
            DownloadFile.downloadFile(downloadUrl, filePath);

        // Reads an Avro file, stores it in a dataframe
        Dataset<Row> df = spark.read()
                .format("avro")
                .load(filePath);

        // Shows at most 10 rows from the dataframe
        df.show(10);
        df.printSchema();
        System.out.println("The dataframe has " + df.count() + " rows.");
    }
}